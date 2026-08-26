// Service pipelines: one long-lived process serving the pipeline's
// input over HTTP. The pipeline declares an internal port (where the
// user's process listens) and an external port (bound on the
// control-plane host). The daemon proxies the external port to the
// process wherever it runs — locally through the execution backend, or
// on a placed execution host through the worker's service endpoints
// (clients only ever need the control-plane host's address).
// New input revisions are re-materialized into the served directory
// without restarting the process.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"sandman/client"
	"sandman/internal/store"
)

// serviceRec is a service pipeline's running record: the declared ports,
// the internal address the proxy forwards to, and the endpoint's
// annotations — the user's own plus the system's identifying pipelineName
// annotation.
type serviceRec struct {
	Pipeline    string            `json:"pipeline"`
	Internal    int               `json:"internalPort"`
	External    int               `json:"externalPort"`
	Annotations map[string]string `json:"annotations"`
	internal    string            `json:"-"` // dial target (host:port)
}

var (
	servicesMu sync.Mutex
	services   = map[string]serviceRec{}
)

func registerService(name string, internalPort, externalPort int, annotations map[string]string, internal string) {
	servicesMu.Lock()
	defer servicesMu.Unlock()
	services[name] = serviceRec{
		Pipeline:    name,
		Internal:    internalPort,
		External:    externalPort,
		Annotations: annotations,
		internal:    internal,
	}
}

func unregisterService(name string) {
	servicesMu.Lock()
	defer servicesMu.Unlock()
	delete(services, name)
}

func serviceRecord(name string) (serviceRec, bool) {
	servicesMu.Lock()
	defer servicesMu.Unlock()
	r, ok := services[name]
	return r, ok
}

// spawnServiceJob starts a service pipeline's single long-lived job.
func (d *daemon) spawnServiceJob(rec *pipelineRec) string {
	id := newJobID(d.name)
	// mirror spawnJob: pre-register the running handle so a
	// stop/delete arriving the instant the service spawns can always
	// find it — a not-yet-registered handle would escape the cancel and
	// keep serving (process up, external port bound) a stopped or
	// deleted pipeline
	rj := d.registerRunning(id, rec.Pipeline.Name)
	go d.runServiceJob(*rec, id, rj)
	return id
}

// runServiceJob runs a service pipeline's one job: materialize the input
// head into the served directory, start the user's process, bind the
// external port and proxy it to the process, and re-materialize the
// served directory whenever the input head advances. The job stays
// running for the process's lifetime and settles when the process exits
// or the job is cancelled. rj is the pre-registered running handle
// (spawnServiceJob) and is unregistered on every exit.
//
// The service is one long-lived process that stays up and serves the
// materialized input over HTTP: the external port is bound on the
// control plane and proxied to the process's internal port, the
// control-plane API exposes a per-pipeline proxy route returning the
// same content as the direct endpoint, user annotations are preserved
// alongside the system's pipelineName annotation, and new input
// revisions are re-served through the same endpoint without restarting
// the process; reachability converges as the process comes up. A placed
// service's endpoint is reachable at the control-plane host's external
// port even though the process runs on a remote execution host: the
// control plane forwards traffic arriving at its external port to the
// remote worker's internal port, so clients only ever need the
// control-plane host's address, and the response is the exact served
// file content served from the pipeline's input data.
func (d *daemon) runServiceJob(pl pipelineRec, id string, rj *runningJob) {
	defer d.unregisterRunning(id, rj)
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	serveDir := filepath.Join(dir, "serve")
	os.MkdirAll(outDir, 0o755)
	os.MkdirAll(serveDir, 0o755)

	rec := newJobRec(pl, nil, id)
	d.saveJob(rec)
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = stateKilled
		rec.Reason = reasonJobCancelled
		rec.Finished = now()
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			// the record may have been deleted while the job was queued
			// (deleteJob removes the whole job directory); never resurrect
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

	// the slot is ours: the queued record becomes running
	rec.State = stateRunning
	d.saveJob(rec)

	// the served input side: the pipeline's first repo side, at its
	// declared name (defaulting to the repo)
	var side client.Input
	for _, s := range inputSides(pl.Pipeline.Input) {
		if s.Repo != "" {
			side = s
			break
		}
	}
	sideName := side.Name
	if sideName == "" {
		sideName = side.Repo
	}
	serveRoot := filepath.Join(serveDir, sideName)

	// a placed service runs on the execution host; the control plane
	// waits for a live host like any placed job (the pipeline
	// surfaces the outage as crashed until a host registers)
	remote := ""
	if pl.Pipeline.Placement != "" {
		for {
			if h, _, ok := d.hosts.pickAndReserve(pl.Pipeline.Placement, 0); ok {
				remote = h.Addr
				break
			}
			d.markPipelineCrashed(pl.Pipeline.Name,
				fmt.Sprintf("no execution host bearing placement label %q", pl.Pipeline.Placement))
			select {
			case <-rj.cancelCh:
				rec.State = stateKilled
				rec.Reason = reasonJobCancelled
				rec.Finished = now()
				d.saveJob(rec)
				return
			case <-time.After(500 * time.Millisecond):
			}
		}
		d.markPipelineRunning(pl.Pipeline.Name)
	}

	// materialize the current input head; a missing head serves an empty
	// directory until the first commit lands
	lastID := ""
	if h, err := d.store.HeadCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished {
		lastID = h.ID
		syncServeDir(d.store, serveRoot, lastID)
	}

	cname := fmt.Sprintf("sandman-%s-service", id)
	rj.registerContainer(cname)
	defer rj.unregisterContainer(cname)

	env := []string{
		"OUT=/sandman/out",
		"JOB_ID=" + id,
		"SERVICE_INTERNAL_PORT=" + strconv.Itoa(pl.Pipeline.Service.InternalPort),
		"SERVICE_EXTERNAL_PORT=" + strconv.Itoa(pl.Pipeline.Service.ExternalPort),
	}
	for k, v := range pl.Pipeline.Transform.Env {
		if !reservedEnv[k] {
			env = append(env, k+"="+v)
		}
	}
	mounts := []string{"-v", outDir + ":/sandman/out", "-v", serveDir + ":/sandman/in"}
	// A LOCAL service container publishes its internal port on the host's
	// loopback so the proxy's 127.0.0.1:<internal> dial reaches it — the
	// remote path publishes on the worker host (worker.go), but without
	// -p a local container sits on the docker bridge, unreachable at the
	// loopback the proxy dials. The process backend ignores these flags.
	// (Must append before the spec captures the slice.)
	mounts = append(mounts, "-p", fmt.Sprintf("127.0.0.1:%d:%d", pl.Pipeline.Service.InternalPort, pl.Pipeline.Service.InternalPort))
	pathMap := map[string]string{"/sandman/out": outDir, "/sandman/in": serveDir}
	spec := JobSpec{
		Image:    pl.Pipeline.Transform.Image,
		NodeName: d.name,
		Name:     cname,
		Cmd:      pl.Pipeline.Transform.Cmd,
		Stdin:    pl.Pipeline.Transform.Stdin,
		Env:      env,
		Mounts:   mounts,
		OutDir:   outDir,
		PathMap:  pathMap,
		Workdir:  pl.Pipeline.Transform.Workdir,
		User:     pl.Pipeline.Transform.User,
	}

	internalPort := pl.Pipeline.Service.InternalPort
	internal := "127.0.0.1:" + strconv.Itoa(internalPort)
	if remote != "" {
		// the service's container publishes its internal port on the
		// worker's host address: dial host:internal, never the worker's
		// exec endpoint
		if host, _, err := net.SplitHostPort(remote); err == nil {
			internal = host + ":" + strconv.Itoa(internalPort)
		}
	}

	ann := map[string]string{"pipelineName": pl.Pipeline.Name}
	for k, v := range pl.Pipeline.Service.Annotations {
		ann[k] = v
	}
	registerService(pl.Pipeline.Name, internalPort, pl.Pipeline.Service.ExternalPort, ann, internal)
	defer unregisterService(pl.Pipeline.Name)

	// start the process: locally through the execution backend, or on
	// the placed host through the worker's service endpoints
	exited := make(chan int, 1)
	var stop func()
	if remote == "" {
		go func() {
			res := d.runner.Run(spec)
			exited <- res.Code
		}()
		stop = func() { d.runner.Kill(cname) }
	} else {
		if err := d.remoteServiceStart(remote, spec, internalPort, side); err != nil {
			// best-effort cleanup: a docker run that failed mid-way (e.g.
			// the port was already taken) can leave a created-but-never-
			// started container holding the port — remove it or every
			// later start fails the same way
			d.remoteServiceStop(remote, cname)
			rec.State = stateFailure
			rec.Reason = "start service on host " + remote + ": " + err.Error()
			rec.Finished = now()
			d.saveJob(rec)
			return
		}
		go func() {
			// the remote service's lifetime is the job's own: it stays
			// up until stopped, so wait for the cancel to release us
			<-rj.cancelCh
			exited <- 0
		}()
		stop = func() { d.remoteServiceStop(remote, cname) }
	}

	// bind the external port and forward it to the process (the process
	// may not be listening yet — the proxy dials per connection, so the
	// endpoint converges as the process comes up)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", pl.Pipeline.Service.ExternalPort))
	if err != nil {
		stop()
		rec.State = stateFailure
		rec.Reason = "bind external port: " + err.Error()
		rec.Finished = now()
		d.saveJob(rec)
		return
	}
	defer ln.Close()
	go proxyListener(ln, internal)

	for {
		// grab-then-check: register the wait channel BEFORE reading the
		// head, so a signal that lands mid-read wakes this select; a
		// signal that lands during the refresh work below is caught by
		// the re-check after it (signal() replaces the channel, so a
		// naive per-iteration registration would lose it — a refresh
		// must not miss a revision)
		ch := d.stateChanged.changed()
		if h, err := d.store.HeadCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished && h.ID != lastID {
			lastID = h.ID
			if remote == "" {
				syncServeDir(d.store, serveRoot, lastID)
			} else {
				if err := d.remoteServiceRefresh(remote, cname, side); err != nil {
					log.Printf("service %s refresh: %v", pl.Pipeline.Name, err)
				}
			}
			continue // the head may have advanced again during the work
		}
		select {
		case <-ch:
		case <-rj.cancelCh:
			stop()
			rec.State = stateKilled
			rec.Reason = reasonJobCancelled
			rec.Finished = now()
			d.saveJob(rec)
			return
		case code := <-exited:
			if rj.cancelled.Load() {
				rec.State = stateKilled
				rec.Reason = reasonJobCancelled
			} else if code == 0 {
				rec.State = stateSuccess
			} else {
				rec.State = stateFailure
				rec.Reason = fmt.Sprintf("service process exited with code %d", code)
			}
			rec.Finished = now()
			d.saveJob(rec)
			return
		}
	}
}

// syncServeDir re-materializes a commit's view into the served directory,
// replacing the previous revision's files (the running process reads from
// disk per request, so the new revision is served immediately).
func syncServeDir(s *store.Store, serveRoot, commitID string) {
	os.RemoveAll(serveRoot)
	os.MkdirAll(serveRoot, 0o755)
	if err := s.MaterializeInput(commitID, serveRoot); err != nil {
		log.Printf("service materialize %s: %v", commitID, err)
	}
}

// proxyListener accepts connections and forwards each to the service's
// internal address: the transport is a plain TCP relay, so any protocol
// the process speaks is served through the external port.
func proxyListener(ln net.Listener, internal string) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func(c net.Conn) {
			up, err := net.Dial("tcp", internal)
			if err != nil {
				c.Close() // the process is not up yet: the request is dropped
				return
			}
			// Half-close relay: the two directions are independent, so the
			// upstream finishing (response complete, or it died) must
			// propagate to the client and vice versa. A WaitGroup coupling
			// both directions deadlocks close-delimited (HTTP/1.0) services
			// behind keep-alive clients: the client holds its side open
			// waiting for the response, so wg.Wait() never returns and the
			// client never sees the close that delimits the body.
			go func() {
				io.Copy(up, c)
				up.Close() // client closed: stop the upstream's write side
			}()
			io.Copy(c, up)
			c.Close() // upstream closed: the client must see it
		}(conn)
	}
}

// ---- service HTTP endpoints ----

// serviceProxyClient forwards service requests with a bounded total
// timeout: a wedged service must fail the proxy request, not hold the
// daemon's handler goroutine (and the client's connection) forever.
var serviceProxyClient = &http.Client{Timeout: 30 * time.Second}

func (d *daemon) serviceProxyH(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	rec, ok := serviceRecord(name)
	if !ok {
		return notFound("service pipeline %q not found", name)
	}
	// forward the remainder of the path — and the query string — to the
	// service's own endpoint: the response is identical to the direct
	// one. A dropped query silently changes the
	// service's behavior (a ?x=1 filter becomes unfiltered).
	rest := r.PathValue("path")
	if r.URL.RawQuery != "" {
		rest += "?" + r.URL.RawQuery
	}
	resp, err := serviceProxyClient.Get("http://" + rec.internal + "/" + rest)
	if err != nil {
		return fmt.Errorf("service %q unreachable: %v", name, err)
	}
	defer resp.Body.Close()
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		return err
	}
	return nil
}

func (d *daemon) inspectServiceH(w http.ResponseWriter, r *http.Request) error {
	rec, ok := serviceRecord(r.PathValue("name"))
	if !ok {
		return notFound("service pipeline %q not found", r.PathValue("name"))
	}
	writeJSON(w, rec)
	return nil
}

func (d *daemon) listServicesH(w http.ResponseWriter, r *http.Request) error {
	servicesMu.Lock()
	defer servicesMu.Unlock()
	out := make([]serviceRec, 0, len(services))
	for _, s := range services {
		out = append(out, s)
	}
	writeJSON(w, out)
	return nil
}

// ---- remote service plumbing ----

// serviceViewFiles returns the side's current head view as shipped files,
// prefixed with the side's served directory name (the service serves
// /<side>/<path>, matching the materialized local mount).
func (d *daemon) serviceViewFiles(side client.Input, headID string) ([]shipFile, error) {
	name := side.Name
	if name == "" {
		name = side.Repo
	}
	view, err := d.store.ResolveViewByID(headID)
	if err != nil {
		return nil, err
	}
	files := make([]shipFile, 0, len(view))
	for p, f := range view {
		data, err := f.Bytes(d.store)
		if err != nil {
			return nil, err
		}
		files = append(files, shipFile{Path: name + "/" + p, Data: data})
	}
	return files, nil
}

// remoteServicePost sends a JSON body to a worker's service endpoint.
// The worker replies 200 with an error field on failure — the body must
// be checked, not just the status. The call is bounded (image pulls can
// be slow, but a wedged worker must not hold the control plane's
// goroutine forever).
func remoteServicePost(host, path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, "http://"+host+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != 200 {
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	var out struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(data, &out) == nil && out.Error != "" {
		return fmt.Errorf("%s", out.Error)
	}
	return nil
}

// remoteServiceStart asks the placed host to run the service process and
// serve the input's current head.
func (d *daemon) remoteServiceStart(host string, spec JobSpec, internalPort int, side client.Input) error {
	var files []shipFile
	if h, err := d.store.HeadCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished {
		if files, err = d.serviceViewFiles(side, h.ID); err != nil {
			return err
		}
	}
	return remoteServicePost(host, "/service", serviceStartRequest{
		Name:         spec.Name,
		Image:        spec.Image,
		Cmd:          spec.Cmd,
		Stdin:        spec.Stdin,
		Env:          spec.Env,
		InternalPort: internalPort,
		Files:        files,
	})
}

// remoteServiceRefresh ships the side's new head view to the running
// service — served without restarting the process.
func (d *daemon) remoteServiceRefresh(host, name string, side client.Input) error {
	h, err := d.store.HeadCommitRec(side.Repo, inputBranch(side))
	if err != nil {
		return err
	}
	files, err := d.serviceViewFiles(side, h.ID)
	if err != nil {
		return err
	}
	return remoteServicePost(host, "/service/refresh", serviceRefreshRequest{Name: name, Files: files})
}

// remoteServiceStop removes the service from the placed host.
func (d *daemon) remoteServiceStop(host, name string) {
	b, _ := json.Marshal(serviceStopRequest{Name: name})
	req, _ := http.NewRequest("DELETE", "http://"+host+"/service", bytes.NewReader(b))
	if req == nil {
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}
