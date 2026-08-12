package main

// The execution-host worker (SB-167/169). One binary, busybox-style: the
// same sandman that runs the control plane also runs `sandman worker`, a
// host-side execution participant. A worker joins the cluster using
// configuration set at host setup time — the control plane's address and
// the join token — and registers itself with its placement labels and its
// own exec endpoint; the pipeline definition never names the host. The
// control plane pushes datum executions to the endpoint; the worker
// materializes the datum, runs the container, and returns the output
// files for the control plane to store into the output commit.

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"sandman/client"
)

// execRequest is the wire contract between the control plane and a worker
// for one datum attempt: the datum's per-side input files, the transform's
// command(s), and the execution parameters (SB-167). Resources follow the
// same mapping as a local run: memory limit → --memory, memory request →
// --memory-reservation, CPU (limit, or request when no limit) → --cpus.
type execRequest struct {
	JobID             string     `json:"jobID"`
	Index             int        `json:"index"`
	Attempt           int        `json:"attempt"`
	Cname             string     `json:"cname"`
	Image             string     `json:"image,omitempty"`
	Cmd               []string   `json:"cmd,omitempty"`
	Stdin             []string   `json:"stdin,omitempty"`
	ErrCmd            []string   `json:"errCmd,omitempty"`
	ErrStdin          []string   `json:"errStdin,omitempty"`
	Env               []string   `json:"env"`
	Sides             []execSide `json:"sides"`
	Memory            string     `json:"memory,omitempty"`
	MemoryReservation string     `json:"memoryReservation,omitempty"`
	CPU               float64    `json:"cpu,omitempty"`
	DatumTimeout      string     `json:"datumTimeout,omitempty"`
	AcceptReturnCode  int        `json:"acceptReturnCode,omitempty"`
	User              string     `json:"user,omitempty"`
	Workdir           string     `json:"workdir,omitempty"`
}

// execSide is one input side's files for the attempt, shipped as content.
type execSide struct {
	Name  string     `json:"name"`
	Files []shipFile `json:"files,omitempty"`
}

// shipFile is one file's content crossing the wire (JSON []byte carries
// base64).
type shipFile struct {
	Path string `json:"path"`
	Data []byte `json:"data"`
}

// execResult is the worker's reply: the primary and error-handler exit
// codes, the output tail, whether the datum timeout killed the run, and
// the produced output files. Error carries a scan/host failure when the
// attempt could not be produced at all.
type execResult struct {
	PrimaryCode int        `json:"primaryCode"`
	ErrCode     int        `json:"errCode,omitempty"`
	Tail        string     `json:"tail,omitempty"`
	TimedOut    bool       `json:"timedOut,omitempty"`
	Outputs     []shipFile `json:"outputs,omitempty"`
	Error       string     `json:"error,omitempty"`
}

// cmdWorker runs the host-side execution participant: register with the
// control plane, heartbeat, and serve the exec endpoint.
func cmdWorker(args []string) {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	name := fs.String("name", "", "host name (required)")
	control := fs.String("control", "", "control plane URL, e.g. http://127.0.0.1:650 (required)")
	port := fs.Int("port", 0, "exec endpoint port (0 = ephemeral)")
	advertise := fs.String("advertise", "", "host:port the control plane must dial to reach this worker (required for placement on a remote host; binds the exec endpoint on all interfaces — the endpoint is unauthenticated, so only set this when the control plane is on another host)")
	var labels strSliceFlag
	fs.Var(&labels, "label", "placement label this host bears (repeatable)")
	fs.Parse(args)
	if *name == "" || *control == "" {
		fmt.Fprintln(os.Stderr, "sandman worker: -name and -control are required")
		os.Exit(2)
	}

	if *advertise != "" {
		if *port == 0 {
			fmt.Fprintln(os.Stderr, "sandman worker: -advertise requires an explicit -port")
			os.Exit(2)
		}
		if _, _, err := net.SplitHostPort(*advertise); err != nil {
			fmt.Fprintf(os.Stderr, "sandman worker: -advertise must be host:port: %v\n", err)
			os.Exit(2)
		}
	}

	// the exec listener binds loopback by default: the endpoint is
	// unauthenticated, so it must not be reachable off-host unless the
	// operator explicitly advertises a reachable address (remote
	// placement, SB-167) — then it listens on every interface so the
	// advertised host is actually reachable, and registers the
	// advertised address instead of the literal listener address (the
	// control plane dials h.Addr for both exec and the service's
	// internal port, so a loopback registration would make the control
	// plane dial ITS OWN loopback on a true remote host).
	bind := fmt.Sprintf("127.0.0.1:%d", *port)
	if *advertise != "" {
		bind = fmt.Sprintf("0.0.0.0:%d", *port)
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		fmt.Fprintf(os.Stderr, "sandman worker: listen: %v\n", err)
		os.Exit(1)
	}
	addr := ln.Addr().String()
	if *advertise != "" {
		addr = *advertise
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /exec", func(w http.ResponseWriter, r *http.Request) {
		var req execRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		writeJSON(w, runExec(*name, req))
	})
	// remote service support (SB-168): the control plane asks the worker
	// to keep a service container alive serving a materialized input
	// directory; the control-plane host proxies its external port to
	// host:internal, so clients never need this worker's address.
	mux.HandleFunc("POST /service", func(w http.ResponseWriter, r *http.Request) {
		var req serviceStartRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := runRemoteService(req); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]string{"ok": "true"})
	})
	mux.HandleFunc("POST /service/refresh", func(w http.ResponseWriter, r *http.Request) {
		var req serviceRefreshRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if err := refreshRemoteService(req); err != nil {
			writeJSON(w, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, map[string]string{"ok": "true"})
	})
	mux.HandleFunc("DELETE /service", func(w http.ResponseWriter, r *http.Request) {
		var req serviceStopRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		stopRemoteService(req.Name)
		writeJSON(w, map[string]string{"ok": "true"})
	})
	// The exec port multiplexes HTTP and the fabric text protocol, like the
	// daemon's port: /exec and /service are HTTP; HELLO/STATS answer the
	// fleet's nodes/stats/dashboard views, so a worker is a first-class
	// fleet citizen (a loopback-bound worker advertises nothing and stays
	// invisible until -advertise makes it reachable).
	apiConns := make(chan net.Conn)
	apiSrv := &http.Server{Handler: mux, ReadHeaderTimeout: 15 * time.Second}
	go apiSrv.Serve(&chanListener{ch: apiConns})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				continue
			}
			go routeConn(c, apiConns, func(c net.Conn, r *bufio.Reader) {
				workerHandleConn(*name, c, r)
			})
		}
	}()

	// Advertise in mDNS only when the worker is reachable off-host
	// (-advertise set): the fleet discovers execution hosts the same way it
	// discovers control planes, and the advertised addr is what consumers
	// dial (never a loopback/bridge browse address). A loopback worker has
	// nothing to advertise.
	if *advertise != "" {
		srv, err := advertiseMDNS(*name, *port, "worker", *advertise)
		if err != nil {
			fmt.Fprintf(os.Stderr, "sandman worker %s: mDNS advertise failed: %v\n", *name, err)
		} else {
			defer srv.Shutdown()
		}
	}
	fmt.Fprintf(os.Stderr, "sandman worker %s: exec endpoint %s, labels %v\n", *name, addr, labels)

	// register, then heartbeat every few seconds: the control plane's
	// host TTL expires a worker that stops reporting, so a vanished host
	// stops receiving work on its own.
	for {
		if err := registerHost(*control, *name, addr, labels); err != nil {
			fmt.Fprintf(os.Stderr, "sandman worker %s: register: %v\n", *name, err)
		}
		time.Sleep(3 * time.Second)
	}
}

// workerHandleConn serves the fabric text protocol on the worker's exec
// port: HELLO/OK handshake, then STATS (the same docker/container reply the
// daemon serves, so nodes/stats/dashboard treat a worker like any other
// node). NODES returns empty — a worker has no peer registry — so the
// daemon's registry-gossip dial completes harmlessly.
func workerHandleConn(name string, c net.Conn, r *bufio.Reader) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second)) // handshake window
	w := bufio.NewWriter(c)

	tok, err := readLine(r)
	if err != nil || len(tok) == 0 || tok[0] != "HELLO" {
		return
	}
	if err := writeLine(w, "OK", "node="+name, "docker="+dockerVersion()); err != nil {
		return
	}

	tok, err = readLine(r)
	if err != nil {
		return
	}
	switch tok[0] {
	case "NODES":
		c.SetDeadline(time.Now().Add(10 * time.Second))
		if err := writeLine(w, "NODES", "0"); err != nil {
			return
		}
	case "STATS":
		c.SetDeadline(time.Now().Add(15 * time.Second))
		// no background cpu ticker on the worker: sample a short window on
		// demand so the dashboard's host-cpu column has a real number
		prev := readCpu()
		time.Sleep(300 * time.Millisecond)
		writeStatsReply(w, cpuBusyDelta(prev, readCpu()))
	}
}

type strSliceFlag []string

func (s *strSliceFlag) String() string { return strings.Join(*s, ",") }
func (s *strSliceFlag) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// registerHost joins (or refreshes) the worker in the control plane's
// host registry.
func registerHost(control, name, addr string, labels []string) error {
	body, err := json.Marshal(map[string]any{"name": name, "addr": addr, "labels": labels})
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", strings.TrimRight(control, "/")+"/api/v1/hosts", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}

// runExec executes one datum attempt on the worker: materialize the
// shipped files, run the primary command (and, when it fails, the
// error-handling command in the same output directory), scan the output,
// and return the produced files. Mirrors the control plane's local
// attempt semantics (SB-012 recovery, SB-113 datum timeout, SB-017 output
// scan) so a placed job's result is identical to a locally run one.
func runExec(nodeName string, req execRequest) execResult {
	dir, err := os.MkdirTemp("", "sandman-worker-*")
	if err != nil {
		return execResult{Error: err.Error()}
	}
	defer os.RemoveAll(dir)
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return execResult{Error: err.Error()}
	}
	tmpDir := filepath.Join(dir, "tmp")
	os.MkdirAll(tmpDir, 0o755)
	env := append([]string{}, req.Env...)
	env = append(env, "HOSTNAME="+nodeName) // the host's identity, visible to the transform
	mounts := []string{"-v", tmpDir + ":/tmp"}
	sideDirs := map[string]string{}
	for _, sd := range req.Sides {
		inDir := filepath.Join(dir, "in", sd.Name)
		if err := os.MkdirAll(inDir, 0o755); err != nil {
			return execResult{Error: err.Error()}
		}
		for _, f := range sd.Files {
			dst := filepath.Join(inDir, filepath.FromSlash(f.Path))
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return execResult{Error: err.Error()}
			}
			if err := os.WriteFile(dst, f.Data, 0o644); err != nil {
				return execResult{Error: err.Error()}
			}
		}
		env = append(env, sd.Name+"=/sandman/in/"+sd.Name)
		mounts = append(mounts, "-v", inDir+":/sandman/in/"+sd.Name+":ro")
		sideDirs[sd.Name] = inDir
	}
	tr := &client.Transform{Image: req.Image, User: req.User, Workdir: req.Workdir}
	if req.Memory != "" || req.CPU > 0 {
		tr.ResourceLimits = &client.ResourceLimits{Memory: req.Memory, CPU: req.CPU}
	}
	if req.MemoryReservation != "" {
		tr.ResourceRequests = &client.ResourceRequests{Memory: req.MemoryReservation}
	}
	cname := req.Cname
	var timedOut atomic.Bool
	if req.DatumTimeout != "" {
		if dur, err := time.ParseDuration(req.DatumTimeout); err == nil {
			time.AfterFunc(dur, func() {
				timedOut.Store(true)
				exec.Command("docker", "kill", cname).Run()
			})
		}
	}
	run := func(cname string, argv, stdin []string) (int, string) {
		if len(argv) == 0 && len(stdin) == 0 {
			// default entry point: copy every side's files to OUT
			code := 0
			for _, sd := range req.Sides {
				if c := copyDir(filepath.Join(dir, "in", sd.Name), outDir); c != 0 {
					code = c
				}
			}
			return code, ""
		}
		return runDatumContainer(tr, nodeName, cname, env, mounts, outDir, nil, argv, stdin)
	}
	primaryCode, tail := run(cname, req.Cmd, req.Stdin)
	// without an error-handling command a nonzero primary exit fails the
	// datum (matching the local path): errCode carries the primary code
	// unless an ErrCmd runs and recovers the datum (SB-012). Defaulting
	// to 0 here silently turned every container failure into a
	// success/recovered datum.
	errCode := primaryCode
	if primaryCode != 0 && (len(req.ErrCmd) > 0 || len(req.ErrStdin) > 0) {
		ec, et := run(cname+"-err", req.ErrCmd, req.ErrStdin)
		errCode, tail = ec, tail+et
	}
	accepted := req.AcceptReturnCode != 0 && primaryCode == req.AcceptReturnCode
	if primaryCode == 0 || accepted || errCode == 0 {
		// symlink resolution for the output scan: /sandman/in/<side> maps
		// to this worker's materialized side dirs, /tmp to its temp dir;
		// the full-side view (/sandman/view) is not shipped to remote
		// hosts, so links into it resolve nowhere (SB-054).
		link := func(target string) string {
			for _, prefix := range []string{"/sandman/in/", "/sandman/view/"} {
				if strings.HasPrefix(target, prefix) {
					rest := strings.TrimPrefix(target, prefix)
					name, file, hasFile := strings.Cut(rest, "/")
					base := sideDirs[name]
					if base == "" {
						return ""
					}
					if !hasFile {
						return base // the whole side (a directory symlink)
					}
					return filepath.Join(base, filepath.FromSlash(file))
				}
			}
			if strings.HasPrefix(target, "/tmp/") {
				return filepath.Join(tmpDir, filepath.FromSlash(strings.TrimPrefix(target, "/tmp/")))
			}
			return ""
		}
		var outputs []shipFile
		if err := walkFiles(outDir, link, func(rel string, data []byte) error {
			outputs = append(outputs, shipFile{Path: rel, Data: data})
			return nil
		}); err != nil {
			return execResult{PrimaryCode: primaryCode, ErrCode: errCode, Tail: tail, TimedOut: timedOut.Load(), Error: "scan output: " + err.Error()}
		}
		if len(tail) > 2000 {
			tail = tail[len(tail)-2000:]
		}
		return execResult{PrimaryCode: primaryCode, ErrCode: errCode, Tail: tail, TimedOut: timedOut.Load(), Outputs: outputs}
	}
	if len(tail) > 2000 {
		tail = tail[len(tail)-2000:]
	}
	return execResult{PrimaryCode: primaryCode, ErrCode: errCode, Tail: tail, TimedOut: timedOut.Load()}
}

// execOnHost pushes one datum attempt to a worker and decodes the result.
func (d *daemon) execOnHost(h *execHost, req execRequest) (code, errCode int, tail string, timedOut bool, outputs []shipFile, err error) {
	b, err := json.Marshal(req)
	if err != nil {
		return 0, 0, "", false, nil, err
	}
	resp, err := http.Post("http://"+h.Addr+"/exec", "application/json", bytes.NewReader(b))
	if err != nil {
		return 0, 0, "", false, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return 0, 0, "", false, nil, fmt.Errorf("host %s: status %d: %s", h.Name, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var res execResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return 0, 0, "", false, nil, err
	}
	if res.Error != "" {
		return 0, 0, "", false, nil, errors.New(res.Error)
	}
	return res.PrimaryCode, res.ErrCode, res.Tail, res.TimedOut, res.Outputs, nil
}

// ---- remote services (SB-168) ----

// serviceStartRequest asks a worker to keep one service container alive
// serving the shipped input files.
type serviceStartRequest struct {
	Name         string     `json:"name"`
	Image        string     `json:"image,omitempty"`
	Cmd          []string   `json:"cmd,omitempty"`
	Stdin        []string   `json:"stdin,omitempty"`
	Env          []string   `json:"env"`
	InternalPort int        `json:"internalPort"`
	Files        []shipFile `json:"files,omitempty"`
}

// serviceRefreshRequest replaces a running service's served input files.
type serviceRefreshRequest struct {
	Name  string     `json:"name"`
	Files []shipFile `json:"files,omitempty"`
}

type serviceStopRequest struct {
	Name string `json:"name"`
}

// workerService tracks one container the worker keeps alive: the host
// directory the container serves.
type workerService struct {
	dir string
}

var (
	workerServicesMu sync.Mutex
	workerServices   = map[string]*workerService{}
)

// runRemoteService starts a detached service container: the input files
// are materialized into a host directory mounted at /sandman/in, and the
// internal port is published on the worker's host address so the control
// plane can proxy to it.
func runRemoteService(req serviceStartRequest) error {
	dir, err := os.MkdirTemp("", "sandman-svc-*")
	if err != nil {
		return err
	}
	if err := writeShippedFiles(dir, req.Files); err != nil {
		os.RemoveAll(dir)
		return err
	}
	outDir := filepath.Join(dir, "out")
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		os.RemoveAll(dir)
		return err
	}
	image := req.Image
	if image == "" {
		image = "alpine"
	}
	args := []string{"run", "-d", "--name", req.Name,
		"-p", fmt.Sprintf("%d:%d", req.InternalPort, req.InternalPort),
		"-v", dir + ":/sandman/in",
		"-v", outDir + ":/sandman/out",
	}
	for _, e := range req.Env {
		args = append(args, "-e", e)
	}
	if len(req.Stdin) > 0 {
		args = append(args, "-i")
	}
	args = append(args, "-w", "/sandman/out", image)
	args = append(args, req.Cmd...)
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		if isProvisioningError(string(out)) {
			return fmt.Errorf("provisioning failed: %s", strings.TrimSpace(string(out)))
		}
		return fmt.Errorf("docker run: %s", strings.TrimSpace(string(out)))
	}
	workerServicesMu.Lock()
	workerServices[req.Name] = &workerService{dir: dir}
	workerServicesMu.Unlock()
	return nil
}

// refreshRemoteService replaces the running service's served input with
// the shipped files: the container's mount reflects them immediately, so
// the service serves the new revision without a restart (SB-100 clause 5).
func refreshRemoteService(req serviceRefreshRequest) error {
	workerServicesMu.Lock()
	svc, ok := workerServices[req.Name]
	workerServicesMu.Unlock()
	if !ok {
		return fmt.Errorf("no service %q on this host", req.Name)
	}
	// clear the served directory's CONTENTS, never the directory itself:
	// svc.dir is the container's bind-mount root, and replacing the
	// directory inode would orphan the mount (the container keeps serving
	// the deleted tree — SB-100 clause 5 refresh must reach it)
	entries, err := os.ReadDir(svc.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(svc.dir, e.Name())); err != nil {
			return err
		}
	}
	return writeShippedFiles(svc.dir, req.Files)
}

// stopRemoteService removes the service container and its materialized
// input; the control plane's proxy then fails to connect and the service
// is gone.
func stopRemoteService(name string) {
	workerServicesMu.Lock()
	svc, ok := workerServices[name]
	delete(workerServices, name)
	workerServicesMu.Unlock()
	if !ok {
		return
	}
	exec.Command("docker", "rm", "-f", name).Run()
	os.RemoveAll(svc.dir)
}

// writeShippedFiles materializes shipped files under dir (paths are
// slash-relative).
func writeShippedFiles(dir string, files []shipFile) error {
	for _, f := range files {
		dst := filepath.Join(dir, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(dst, f.Data, 0o644); err != nil {
			return err
		}
	}
	return nil
}
