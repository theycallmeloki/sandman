// Service pipelines (SB-100/168): one long-lived process serving the
// pipeline's input over HTTP. The pipeline declares an internal port
// (where the user's process listens) and an external port (bound on the
// control-plane host). The daemon proxies the external port to the
// process wherever it runs — locally through the execution backend, or
// on a placed execution host through the worker's service endpoints
// (SB-168: clients only ever need the control-plane host's address).
// New input revisions are re-materialized into the served directory
// without restarting the process (SB-100 clause 5).
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
)

// serviceRec is a service pipeline's running record: the declared ports,
// the internal address the proxy forwards to, and the endpoint's
// annotations — the user's own plus the system's identifying pipelineName
// annotation (SB-100 clause 4).
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
	go d.runServiceJob(*rec, id)
	return id
}

// runServiceJob runs a service pipeline's one job: materialize the input
// head into the served directory, start the user's process, bind the
// external port and proxy it to the process, and re-materialize the
// served directory whenever the input head advances. The job stays
// running for the process's lifetime and settles when the process exits
// or the job is cancelled.
func (d *daemon) runServiceJob(pl pipelineRec, id string) {
	dir := d.jobDir(id)
	outDir := filepath.Join(dir, "out")
	serveDir := filepath.Join(dir, "serve")
	os.MkdirAll(outDir, 0o755)
	os.MkdirAll(serveDir, 0o755)

	rj := registerRunning(id, pl.Pipeline.Name)
	defer unregisterRunning(id, rj)

	rec := newJobRec(pl, nil, id)
	d.saveJob(rec)
	gate := d.jobGate(pl.Pipeline.Name)
	if !gate.enter(rj) {
		rec.State = "killed"
		rec.Reason = "job cancelled"
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
		if _, err := os.Stat(filepath.Join(dir, "job.json")); err == nil {
			// the record may have been deleted while the job was queued
			// (deleteJob removes the whole job directory); never resurrect
			d.saveJob(rec)
		}
		return
	}
	defer gate.release()

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
	// waits for a live host like any placed job (SB-169: the pipeline
	// surfaces the outage as crashed until a host registers)
	remote := ""
	if pl.Pipeline.Placement != "" {
		for {
			if h, ok := d.hosts.pick(pl.Pipeline.Placement); ok {
				remote = h.Addr
				break
			}
			d.markPipelineCrashed(pl.Pipeline.Name,
				fmt.Sprintf("no execution host bearing placement label %q", pl.Pipeline.Placement))
			select {
			case <-rj.cancelCh:
				rec.State = "killed"
				rec.Reason = "job cancelled"
				rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
	if h, err := d.store.headCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished {
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
		// exec endpoint (SB-168)
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
			rec.State = "failure"
			rec.Reason = "start service on host " + remote + ": " + err.Error()
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
	// endpoint converges as the process comes up, SB-100/168 clause 4)
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", pl.Pipeline.Service.ExternalPort))
	if err != nil {
		stop()
		rec.State = "failure"
		rec.Reason = "bind external port: " + err.Error()
		rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
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
		// naive per-iteration registration would lose it — SB-100 clause
		// 5 must not miss a revision)
		ch := d.stateChanged.changed()
		if h, err := d.store.headCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished && h.ID != lastID {
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
			rec.State = "killed"
			rec.Reason = "job cancelled"
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			d.saveJob(rec)
			return
		case code := <-exited:
			if rj.cancelled.Load() {
				rec.State = "killed"
				rec.Reason = "job cancelled"
			} else if code == 0 {
				rec.State = "success"
			} else {
				rec.State = "failure"
				rec.Reason = fmt.Sprintf("service process exited with code %d", code)
			}
			rec.Finished = time.Now().UTC().Format(time.RFC3339Nano)
			d.saveJob(rec)
			return
		}
	}
}

// syncServeDir re-materializes a commit's view into the served directory,
// replacing the previous revision's files (the running process reads from
// disk per request, so the new revision is served immediately).
func syncServeDir(s *apiStore, serveRoot, commitID string) {
	os.RemoveAll(serveRoot)
	os.MkdirAll(serveRoot, 0o755)
	if err := s.materializeInput(commitID, serveRoot); err != nil {
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

func (d *daemon) serviceProxyH(w http.ResponseWriter, r *http.Request) error {
	name := r.PathValue("pipeline")
	rec, ok := serviceRecord(name)
	if !ok {
		return fmt.Errorf("service pipeline %q not found", name)
	}
	// forward the remainder of the path to the service's own endpoint:
	// the response is identical to the direct one (SB-100 clause 3)
	rest := r.PathValue("path")
	resp, err := http.Get("http://" + rec.internal + "/" + rest)
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
		return fmt.Errorf("service pipeline %q not found", r.PathValue("name"))
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

// ---- remote service plumbing (SB-168) ----

// serviceViewFiles returns the side's current head view as shipped files,
// prefixed with the side's served directory name (the service serves
// /<side>/<path>, matching the materialized local mount).
func (d *daemon) serviceViewFiles(side client.Input, headID string) ([]shipFile, error) {
	name := side.Name
	if name == "" {
		name = side.Repo
	}
	view, err := d.store.resolveViewByID(headID)
	if err != nil {
		return nil, err
	}
	files := make([]shipFile, 0, len(view))
	for p, f := range view {
		data, err := f.bytes(d.store)
		if err != nil {
			return nil, err
		}
		files = append(files, shipFile{Path: name + "/" + p, Data: data})
	}
	return files, nil
}

// remoteServicePost sends a JSON body to a worker's service endpoint.
// The worker replies 200 with an error field on failure — the body must
// be checked, not just the status.
func remoteServicePost(host, path string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := http.Post("http://"+host+path, "application/json", bytes.NewReader(b))
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
	if h, err := d.store.headCommitRec(side.Repo, inputBranch(side)); err == nil && h.Finished {
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
// service — served without restarting the process (SB-100 clause 5).
func (d *daemon) remoteServiceRefresh(host, name string, side client.Input) error {
	h, err := d.store.headCommitRec(side.Repo, inputBranch(side))
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
