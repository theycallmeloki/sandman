package main

// The HTTP API surface (/api/v1/...) lives on the same TCP port as the
// fabric's text protocol. The accept loop peeks the first bytes of each
// connection: an HTTP method line is handed to the API server, anything
// else (HELLO…) to the text protocol. One port, one discovery story, two
// protocols — each connection routed by its own shape.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sandman/client"
)

// isHTTPMethod reports whether the peeked head of a connection is an HTTP
// request line rather than the fabric text protocol (HELLO first).
func isHTTPMethod(head []byte) bool {
	for _, m := range []string{"GET ", "POST", "PUT ", "DELE", "HEAD", "PATC", "OPTI", "CONN", "TRAC"} {
		if len(head) >= 4 && string(head[:4]) == m[:4] {
			return true
		}
	}
	return false
}

// chanListener delivers pre-routed connections to http.Server.
type chanListener struct {
	ch chan net.Conn
}

func (l *chanListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok {
		return nil, errors.New("listener closed")
	}
	return c, nil
}
func (l *chanListener) Close() error   { return nil }
func (l *chanListener) Addr() net.Addr { return dummyAddr{} }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "tcp" }
func (dummyAddr) String() string  { return "sandman" }

// replayConn hands the peeked bytes back to the consumer: the buffered
// reader already holds them, so all reads go through it.
type replayConn struct {
	net.Conn
	r *bufio.Reader
}

func (c replayConn) Read(p []byte) (int, error) { return c.r.Read(p) }

// serveConn routes one accepted connection by its first four bytes.
func (d *daemon) serveConn(c net.Conn, apiConns chan<- net.Conn) {
	routeConn(c, apiConns, d.handleConn)
}

// routeConn splits one accepted connection by its first four bytes: HTTP
// goes to the api server (via the channel listener), anything else is the
// fabric text protocol and goes to the text handler. The daemon and the
// worker share the splitter — both serve HTTP + text on one port.
func routeConn(c net.Conn, apiConns chan<- net.Conn, text func(net.Conn, *bufio.Reader)) {
	br := bufio.NewReader(c)
	c.SetDeadline(time.Now().Add(30 * time.Second)) // handshake window
	head, err := br.Peek(4)
	if err != nil {
		c.Close()
		return
	}
	if isHTTPMethod(head) {
		c.SetDeadline(time.Time{}) // http.Server manages its own timeouts
		apiConns <- replayConn{c, br}
		return
	}
	text(c, br)
}

func (d *daemon) apiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/repos", hErr(d.createRepoH))
	mux.HandleFunc("GET /api/v1/repos", hErr(d.listReposH))
	mux.HandleFunc("GET /api/v1/repos/{name}", hErr(d.inspectRepoH))
	mux.HandleFunc("DELETE /api/v1/repos/{name}", hErr(d.deleteRepoH))
	mux.HandleFunc("POST /api/v1/repos/{name}/commits", hErr(d.startCommitH))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches/{branch}/head", hErr(d.headCommitH))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches", hErr(d.listBranchesH))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches/{branch}", hErr(d.inspectBranchH))
	mux.HandleFunc("DELETE /api/v1/repos/{name}/branches/{branch}", hErr(d.deleteBranchH))
	mux.HandleFunc("POST /api/v1/repos/{name}/branches/{branch}", hErr(d.createBranchH))
	mux.HandleFunc("POST /api/v1/commits/{id}/finish", hErr(d.finishCommitH))
	mux.HandleFunc("GET /api/v1/commits/{id}", hErr(d.inspectCommitH))
	mux.HandleFunc("PUT /api/v1/commits/{id}/files/{path...}", d.instrument("write", hErr(d.putFileH)))
	mux.HandleFunc("POST /api/v1/commits/{id}/files/{path...}", hErr(d.copyFileH))
	mux.HandleFunc("DELETE /api/v1/commits/{id}/files/{path...}", hErr(d.deleteFileH))
	mux.HandleFunc("GET /api/v1/commits/{id}/files/{path...}", d.instrument("read", hErr(d.getFileH)))
	mux.HandleFunc("GET /api/v1/commits/{id}/files", hErr(d.listFilesH))
	mux.HandleFunc("DELETE /api/v1/commits/{id}", hErr(d.deleteCommitH))
	mux.HandleFunc("POST /api/v1/secrets", hErr(d.createSecretH))
	mux.HandleFunc("GET /api/v1/secrets", hErr(d.listSecretsH))
	mux.HandleFunc("GET /api/v1/secrets/{name}", hErr(d.inspectSecretH))
	mux.HandleFunc("DELETE /api/v1/secrets/{name}", hErr(d.deleteSecretH))
	mux.HandleFunc("POST /api/v1/hosts", hErr(d.registerHostH))
	mux.HandleFunc("GET /api/v1/hosts", hErr(d.listHostsH))
	mux.HandleFunc("DELETE /api/v1/hosts/{name}", hErr(d.deleteHostH))
	mux.HandleFunc("GET /api/v1/metrics", hErr(d.metricsH))
	mux.HandleFunc("GET /api/v1/version", hErr(d.versionH))
	mux.HandleFunc("POST /api/v1/gc", hErr(d.collectGarbageH))
	mux.HandleFunc("POST /api/v1/check", hErr(d.checkH))
	mux.HandleFunc("POST /api/v1/pipelines", hErr(d.createPipelineH))
	mux.HandleFunc("GET /api/v1/pipelines", hErr(d.listPipelinesH))
	mux.HandleFunc("GET /api/v1/pipelines/{name}", hErr(d.inspectPipelineH))
	mux.HandleFunc("DELETE /api/v1/pipelines/{name}", hErr(d.deletePipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/stop", hErr(d.stopPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/start", hErr(d.startPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/run", hErr(d.runPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/trigger", hErr(d.triggerCronH))
	mux.HandleFunc("GET /api/v1/jobs", d.instrument("listJobs", hErr(d.listJobsH)))
	mux.HandleFunc("GET /api/v1/jobs/{id}", hErr(d.inspectJobH))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums", hErr(d.listDatumsH))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums/{datumID}", hErr(d.inspectDatumH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/datums/{datumID}/restart", hErr(d.restartDatumH))
	mux.HandleFunc("POST /api/v1/git/push", hErr(d.gitPushH))
	mux.HandleFunc("POST /api/v1/flush", hErr(d.flushH))
	mux.HandleFunc("GET /api/v1/jobs/{id}/wait", hErr(d.jobWaitH))
	mux.HandleFunc("GET /api/v1/services", hErr(d.listServicesH))
	mux.HandleFunc("GET /api/v1/services/{name}", hErr(d.inspectServiceH))
	mux.HandleFunc("GET /api/v1/services/{pipeline}/{path...}", hErr(d.serviceProxyH))
	mux.HandleFunc("GET /api/v1/logs", hErr(d.logsH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", hErr(d.cancelJobH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/stop", hErr(d.cancelJobH))
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", hErr(d.deleteJobH))
	mux.HandleFunc("POST /api/v1/transactions", hErr(d.startTransactionH))
	mux.HandleFunc("GET /api/v1/transactions", hErr(d.listTransactionsH))
	mux.HandleFunc("GET /api/v1/transactions/{id}", hErr(d.inspectTransactionH))
	mux.HandleFunc("POST /api/v1/transactions/{id}/finish", hErr(d.finishTransactionH))
	mux.HandleFunc("DELETE /api/v1/transactions/{id}", hErr(d.deleteTransactionH))
	mux.HandleFunc("POST /api/v1/datums", hErr(d.enumerateDatumsH))
	mux.HandleFunc("POST /api/v1/reset", hErr(d.resetH))
	mux.HandleFunc("PUT /api/v1/tags/{name}", hErr(d.putTagH))
	mux.HandleFunc("GET /api/v1/tags/{name}", hErr(d.getTagH))
	mux.HandleFunc("DELETE /api/v1/tags/{name}", hErr(d.deleteTagH))
	mux.HandleFunc("GET /api/v1/tags", hErr(d.listTagsH))
	return mux
}

// hErr wraps a handler that returns a client error.
type handler func(w http.ResponseWriter, r *http.Request) error

func hErr(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			code := http.StatusBadRequest
			msg := err.Error()
			if strings.Contains(msg, "not found") {
				code = http.StatusNotFound
			}
			writeErr(w, code, msg)
		}
	}
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func decodeBody(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 1<<30)).Decode(v)
}

// ---- repos ----

func (d *daemon) createRepoH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Name string `json:"name"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if err := d.store.createRepo(body.Name); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"name": body.Name})
	return nil
}

func (d *daemon) listReposH(w http.ResponseWriter, r *http.Request) error {
	repos, err := d.store.listRepos()
	if err != nil {
		return err
	}
	writeJSON(w, repos)
	return nil
}

func (d *daemon) inspectRepoH(w http.ResponseWriter, r *http.Request) error {
	repo, err := d.store.inspectRepo(r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, repo)
	return nil
}

func (d *daemon) deleteRepoH(w http.ResponseWriter, r *http.Request) error {
	return d.store.deleteRepo(r.PathValue("name"), r.URL.Query().Get("force") == "1")
}

// ---- commits ----

func (d *daemon) startCommitH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Branch      string `json:"branch"`
		Description string `json:"description"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	cm, err := d.store.startCommit(r.PathValue("name"), body.Branch, body.Description)
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	return nil
}

func (d *daemon) headCommitH(w http.ResponseWriter, r *http.Request) error {
	cm, err := d.store.headCommitRec(r.PathValue("name"), r.PathValue("branch"))
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	return nil
}

// createBranchH points a branch at an existing commit, creating the branch
// or retargeting it (SB-142).
func (d *daemon) createBranchH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Head string `json:"head"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if err := d.createBranch(r.PathValue("name"), r.PathValue("branch"), body.Head); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// listBranchesH lists the repo's branches with their head commit ids.
func (d *daemon) listBranchesH(w http.ResponseWriter, r *http.Request) error {
	bs, err := d.store.branchRefs(r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, bs)
	return nil
}

// inspectBranchH returns the named branch's head commit id.
func (d *daemon) inspectBranchH(w http.ResponseWriter, r *http.Request) error {
	head, err := d.store.branchHead(r.PathValue("name"), r.PathValue("branch"))
	if err != nil {
		return err
	}
	writeJSON(w, client.Branch{Repo: r.PathValue("name"), Branch: r.PathValue("branch"), Head: head})
	return nil
}

// deleteBranchH removes the branch ref (the default branch is protected).
func (d *daemon) deleteBranchH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.deleteBranch(r.PathValue("name"), r.PathValue("branch")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// createBranch points a branch at an existing commit — creating the branch
// or retargeting it. Pipelines watch branch heads, so the retarget is
// itself a trigger: the commit is now on the watched branch and is
// processed exactly once (SB-142).
func (d *daemon) createBranch(repo, branch, head string) error {
	if branch == "" {
		return fmt.Errorf("branch must specify a name")
	}
	rec, err := d.store.loadCommitByID(head)
	if err != nil {
		return fmt.Errorf("commit %q not found", head)
	}
	if rec.Repo != repo {
		return fmt.Errorf("commit %s is not in repo %q", head, repo)
	}
	if err := d.store.setHead(repo, branch, head); err != nil {
		return err
	}
	cm := rec.commit()
	cm.Branch = branch // the trigger keys off the watched branch, not the commit's origin
	d.triggerForCommit(cm)
	return nil
}

func (d *daemon) finishCommitH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Description string `json:"description"`
		Empty       bool   `json:"empty"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	cm, err := d.store.finishCommit(r.PathValue("id"), body.Description, body.Empty)
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	d.triggerForCommit(cm)
	return nil
}

func (d *daemon) inspectCommitH(w http.ResponseWriter, r *http.Request) error {
	cm, err := d.store.inspectCommit(r.PathValue("id"))
	if err != nil {
		return err
	}
	// Subvenants are the commits that derive from this one: every commit
	// whose recorded provenance includes it (SB-140 — a spec commit's
	// subvenants are its pipeline's spout output and the downstream
	// output; an epoch's commits all derive from their spec commit). The
	// scan is unconditional: the inspected commit needs no provenance of
	// its own to be derived from.
	for _, c := range d.allCommitRecs() {
		if c.ID == cm.ID {
			continue
		}
		for _, p := range c.Provenance {
			if p == cm.ID {
				cm.Subvenants = append(cm.Subvenants, c.ID)
				break
			}
		}
	}
	writeJSON(w, cm)
	return nil
}

// deleteCommitH deletes a commit (by id or repo@branch reference) and
// everything derived from it across the DAG (SB-124/125).
func (d *daemon) deleteCommitH(w http.ResponseWriter, r *http.Request) error {
	if err := d.deleteCommit(r.PathValue("id")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- runtime metrics (SB-132) ----

// hist is a latency histogram's aggregate: a sum and a count, so an
// average is computable.
type hist struct {
	sum   float64
	count int64
}

// metricsStore accumulates the instrumented operations' invocation counts
// and latency aggregates. File-read latency is split by outcome (SB-132).
type metricsStore struct {
	mu         sync.Mutex
	readTotal  int64
	readOK     hist // successful file reads
	readErr    hist // errored file reads
	write      hist
	writeTotal int64
	list       hist
	listTotal  int64
}

func (m *metricsStore) observeRead(dur float64, err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readTotal++
	if err {
		m.readErr.sum += dur
		m.readErr.count++
	} else {
		m.readOK.sum += dur
		m.readOK.count++
	}
}

func (m *metricsStore) observeWrite(dur float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeTotal++
	m.write.sum += dur
	m.write.count++
}

func (m *metricsStore) observeList(dur float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listTotal++
	m.list.sum += dur
	m.list.count++
}

// statusRecorder captures the response status so instrumentation can tell
// a successful invocation from an errored one.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// instrument wraps an HTTP handler with its operation's invocation counter
// and latency histogram (SB-132).
func (d *daemon) instrument(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, status: 200}
		h(sw, r)
		dur := time.Since(start).Seconds()
		switch op {
		case "read":
			d.metrics.observeRead(dur, sw.status >= 400)
		case "write":
			d.metrics.observeWrite(dur)
		case "listJobs":
			d.metrics.observeList(dur)
		}
	}
}

// metricsH renders the runtime metrics in Prometheus exposition format:
// invocation counters and latency sum/count aggregates for file reads,
// file writes, and job listings, with read latency split by outcome
// (SB-132).
// versionH reports the daemon's baked build version (the same Version the
// binary carries; a `sandman --version` shows both so a stale daemon is
// visible).
func (d *daemon) versionH(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, map[string]string{"version": Version})
	return nil
}

func (d *daemon) metricsH(w http.ResponseWriter, r *http.Request) error {
	d.metrics.mu.Lock()
	readTotal := d.metrics.readTotal
	readOK, readErr := d.metrics.readOK, d.metrics.readErr
	writeTotal, writeH := d.metrics.writeTotal, d.metrics.write
	listTotal, listH := d.metrics.listTotal, d.metrics.list
	d.metrics.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP sandbox_file_read_total File read invocations.\n# TYPE sandbox_file_read_total counter\nsandbox_file_read_total %d\n", readTotal)
	fmt.Fprintf(w, "# TYPE sandbox_file_read_seconds histogram\n")
	fmt.Fprintf(w, "sandbox_file_read_seconds_sum{outcome=\"success\"} %g\n", readOK.sum)
	fmt.Fprintf(w, "sandbox_file_read_seconds_count{outcome=\"success\"} %d\n", readOK.count)
	fmt.Fprintf(w, "sandbox_file_read_seconds_sum{outcome=\"error\"} %g\n", readErr.sum)
	fmt.Fprintf(w, "sandbox_file_read_seconds_count{outcome=\"error\"} %d\n", readErr.count)
	fmt.Fprintf(w, "# HELP sandbox_file_write_total File write invocations.\n# TYPE sandbox_file_write_total counter\nsandbox_file_write_total %d\n", writeTotal)
	fmt.Fprintf(w, "# TYPE sandbox_file_write_seconds histogram\n")
	fmt.Fprintf(w, "sandbox_file_write_seconds_sum %g\n", writeH.sum)
	fmt.Fprintf(w, "sandbox_file_write_seconds_count %d\n", writeH.count)
	fmt.Fprintf(w, "# HELP sandbox_job_list_total Job listing invocations.\n# TYPE sandbox_job_list_total counter\nsandbox_job_list_total %d\n", listTotal)
	fmt.Fprintf(w, "# TYPE sandbox_job_list_seconds histogram\n")
	fmt.Fprintf(w, "sandbox_job_list_seconds_sum %g\n", listH.sum)
	fmt.Fprintf(w, "sandbox_job_list_seconds_count %d\n", listH.count)
	return nil
}

// ---- garbage collection (SB-079, D-20) ----

// checkH is the consistency check (SB-139 clause 14): every piece of
// control-plane metadata parses; a corrupted record is reported as an
// error, an intact system reports ok. A system-wide reset (POST
// /api/v1/reset) runs the same check first (D-08).
func (d *daemon) checkH(w http.ResponseWriter, r *http.Request) error {
	if err := d.checkMetadata(); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// collectGarbageH is the manual collection trigger (D-20: automatic
// collection defaults off).
func (d *daemon) collectGarbageH(w http.ResponseWriter, r *http.Request) error {
	if err := d.collectGarbage(); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// collectGarbage reclaims durable artifacts no longer referenced by any
// commit tree, tag, or spec record (SB-079). It refuses while a job is
// running: active processing may still be about to read the data. Only
// unreferenced blobs are removed — reachable data is never touched.
func (d *daemon) collectGarbage() error {
	for _, j := range d.mustListJobs() {
		if j.State == "running" {
			return fmt.Errorf("cannot collect garbage while a job is running")
		}
	}
	referenced := map[string]bool{}
	for _, cm := range d.allCommitRecs() {
		for _, op := range cm.Ops {
			if op.SHA != "" {
				referenced[op.SHA] = true
			}
		}
	}
	// tags hold a reference to their blob (SB-150)
	if entries, err := os.ReadDir(filepath.Join(d.state, "tags")); err == nil {
		for _, e := range entries {
			if b, err := os.ReadFile(filepath.Join(d.state, "tags", e.Name())); err == nil {
				referenced[strings.TrimSpace(string(b))] = true
			}
		}
	}
	objects := filepath.Join(d.state, "repos", ".objects")
	entries, err := os.ReadDir(objects)
	if err != nil {
		return nil // nothing stored
	}
	removed := 0
	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != 2 {
			continue
		}
		sub, err := os.ReadDir(filepath.Join(objects, e.Name()))
		if err != nil {
			continue
		}
		for _, b := range sub {
			sha := e.Name() + b.Name()
			if referenced[sha] {
				continue
			}
			if os.Remove(filepath.Join(objects, e.Name(), b.Name())) == nil {
				removed++
			}
		}
	}
	log.Printf("garbage collection removed %d unreferenced objects", removed)
	return nil
}

// ---- secrets (SB-153/154) ----

// requireAuth enforces the daemon's configured credential on the
// management endpoints that require one: a request without the token is
// rejected with "no authentication token"; a wrong token is rejected too
// (SB-154). With no token configured, authentication is disabled.
func (d *daemon) requireAuth(r *http.Request) error {
	if d.authToken == "" {
		return nil
	}
	got := r.Header.Get("X-Sandbox-Token")
	if got == "" {
		return fmt.Errorf("no authentication token")
	}
	if got != d.authToken {
		return fmt.Errorf("invalid authentication token")
	}
	return nil
}

// secretRec is a secret's durable record: a named metadata blob with a
// type label and key/value data (SB-153, D-05 — durable, like every other
// meta-plane record).
type secretRec struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Created string            `json:"created"`
	Data    map[string]string `json:"data,omitempty"`
}

func (d *daemon) secretPath(name string) string {
	return filepath.Join(d.state, "secrets", name+".json")
}

func (d *daemon) createSecretH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	var body struct {
		Name string            `json:"name"`
		Data map[string]string `json:"data"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if body.Name == "" {
		return fmt.Errorf("secret must specify a name")
	}
	rec := secretRec{
		Name:    body.Name,
		Type:    "Opaque",
		Created: time.Now().UTC().Format(time.RFC3339Nano),
		Data:    body.Data,
	}
	if err := os.MkdirAll(filepath.Join(d.state, "secrets"), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if err := os.WriteFile(d.secretPath(body.Name), b, 0o644); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) inspectSecretH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	rec, err := d.loadSecret(r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, client.SecretInfo{Name: rec.Name, Type: rec.Type, Created: rec.Created})
	return nil
}

func (d *daemon) loadSecret(name string) (*secretRec, error) {
	b, err := os.ReadFile(d.secretPath(name))
	if err != nil {
		return nil, fmt.Errorf("secret %q not found", name)
	}
	var rec secretRec
	if json.Unmarshal(b, &rec) != nil {
		return nil, fmt.Errorf("secret %q is corrupt", name)
	}
	return &rec, nil
}

func (d *daemon) listSecretsH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	var out []client.SecretInfo
	entries, err := os.ReadDir(filepath.Join(d.state, "secrets"))
	if err == nil {
		for _, e := range entries {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			if rec, err := d.loadSecret(strings.TrimSuffix(e.Name(), ".json")); err == nil {
				out = append(out, client.SecretInfo{Name: rec.Name, Type: rec.Type, Created: rec.Created})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	writeJSON(w, out)
	return nil
}

func (d *daemon) deleteSecretH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	os.Remove(d.secretPath(r.PathValue("name"))) // idempotent in effect (SB-153)
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- execution hosts (SB-167/169) ----

// registerHostH is the join endpoint an execution host calls at setup and
// on its heartbeat: the worker reports its name, its exec endpoint, and
// the placement labels it bears. The control plane schedules labeled
// pipelines onto registered hosts; a pipeline definition never names a
// host address (SB-167).
func (d *daemon) registerHostH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	var body struct {
		Name   string   `json:"name"`
		Addr   string   `json:"addr"`
		Labels []string `json:"labels"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if body.Name == "" || body.Addr == "" {
		return fmt.Errorf("host registration needs a name and an address")
	}
	h := d.hosts.register(body.Name, body.Addr, body.Labels)
	writeJSON(w, h)
	return nil
}

func (d *daemon) listHostsH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	writeJSON(w, d.hosts.list())
	return nil
}

func (d *daemon) deleteHostH(w http.ResponseWriter, r *http.Request) error {
	if err := d.requireAuth(r); err != nil {
		return err
	}
	d.hosts.drop(r.PathValue("name"))
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- files ----

func (d *daemon) putFileH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	// fetch=URL: ingest a remote file (SB-088) — the URL's body becomes
	// the file's content
	if u := q.Get("fetch"); u != "" {
		resp, err := http.Get(u)
		if err != nil {
			return fmt.Errorf("fetch %s: %v", u, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("fetch %s: status %d", u, resp.StatusCode)
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		if err != nil {
			return fmt.Errorf("read fetch: %v", err)
		}
		if err := d.store.putFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
			return err
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<30))
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}
	// split=1&delimiter=X[&header=1]: split the upload into records at
	// the delimiter; with a header, the first chunk is the header and is
	// replicated into every record's file, each stored at path/<i>
	// (SB-137/138 — same-header appends leave earlier records' identity
	// unchanged, so they are skipped by the dedup)
	if q.Get("split") == "1" {
		delim := q.Get("delimiter")
		header := q.Get("header") == "1"
		chunks := strings.Split(string(data), delim)
		start := 0
		if header {
			start = 1
		}
		records := chunks[start:]
		prefix := r.PathValue("path") + "/"
		base := 0
		var firstHeader string
		if view, err := d.store.resolveViewByID(r.PathValue("id")); err == nil {
			for p := range view {
				if strings.HasPrefix(p, prefix) {
					base++
				}
			}
		}
		// a changed header re-identifies every record: the existing record
		// paths are overwritten with the new header + their existing record
		// content (FS-7 — the header swaps everywhere without changing the
		// record count or numbering), so all are reprocessed (SB-138)
		changed := false
		if header && base > 0 {
			if first, err := d.store.getFile(r.PathValue("id"), prefix+"0"); err == nil {
				firstHeader = string(first)
				if i := strings.IndexByte(firstHeader, '\n'); i >= 0 {
					firstHeader = firstHeader[:i]
				}
				changed = firstHeader != chunks[0]
			}
		}
		if changed {
			// swap the header into every existing record, keeping its path
			// (record content after the first delimiter); the upload's own
			// records only supply extra records beyond the existing count
			for i := 0; i < base; i++ {
				stored, err := d.store.getFile(r.PathValue("id"), prefix+strconv.Itoa(i))
				if err != nil {
					return err
				}
				rec := string(stored)
				if j := strings.Index(rec, delim); j >= 0 {
					rec = rec[j+len(delim):]
				}
				if err := d.store.overwriteFile(r.PathValue("id"), prefix+strconv.Itoa(i), []byte(chunks[0]+delim+rec)); err != nil {
					return err
				}
			}
			records = records[min(base, len(records)):]
		}
		// new records continue the numbering after the existing records,
		// whether or not the header changed (FS-7)
		off := base
		for i, rec := range records {
			content := rec
			if header {
				content = chunks[0] + delim + content
			}
			if err := d.store.putFile(r.PathValue("id"), prefix+strconv.Itoa(off+i), []byte(content)); err != nil {
				return err
			}
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	if q.Get("overwrite") == "1" {
		if err := d.store.overwriteFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
			return err
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	if err := d.store.putFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) getFileH(w http.ResponseWriter, r *http.Request) error {
	// history=1 turns the read into a revision-history listing (SB-145):
	// one FileInfo per ancestor revision where the path resolves, newest
	// first, capped by limit (negative = every revision)
	if r.URL.Query().Get("history") == "1" {
		limit := -1
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil {
				limit = n
			}
		}
		hist, err := d.fileHistory(r.PathValue("id"), r.PathValue("path"), limit)
		if err != nil {
			return err
		}
		writeJSON(w, hist)
		return nil
	}
	data, err := d.store.getFile(r.PathValue("id"), r.PathValue("path"))
	if err != nil {
		return err
	}
	// Content type is detected from the bytes, not a stored label (SB-099).
	w.Header().Set("Content-Type", http.DetectContentType(data))
	if r.URL.Query().Get("download") == "true" {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filepath.Base(r.PathValue("path"))))
	}
	_, _ = w.Write(data)
	return nil
}

// fileHistory lists the revisions of a path across the commit's ancestry:
// one FileInfo per ancestor revision where the path resolves, newest
// first, capped at limit (negative = every revision). A cross input's
// multi-commit provenance is just more ancestry to walk (SB-145).
func (d *daemon) fileHistory(commitID, path string, limit int) ([]client.FileInfo, error) {
	rec, err := d.store.loadCommitByID(commitID)
	if err != nil {
		return nil, err
	}
	var out []client.FileInfo
	for cur := rec; cur != nil; {
		if f, ok := d.store.resolveView(cur)[path]; ok {
			h, err := f.hash(d.store)
			if err != nil {
				return nil, err
			}
			out = append(out, client.FileInfo{Path: path, Size: f.size(), Hash: h})
			if limit >= 0 && len(out) >= limit {
				break
			}
		}
		if cur.ParentID == "" {
			break
		}
		parent, err := d.store.loadCommit(cur.Repo, cur.ParentID)
		if err != nil {
			break
		}
		cur = parent
	}
	return out, nil
}

func (d *daemon) copyFileH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		SrcCommit string `json:"srcCommit"`
		SrcPath   string `json:"srcPath"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	overwrite := r.URL.Query().Get("overwrite") == "1"
	if err := d.store.copyFile(r.PathValue("id"), r.PathValue("path"), body.SrcCommit, body.SrcPath, overwrite); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) deleteFileH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.deleteFile(r.PathValue("id"), r.PathValue("path")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// enumerateDatumsH serves POST /api/v1/datums: the datum set an input
// would process at its sides' current heads, without creating or running a
// pipeline (SB-161).
func (d *daemon) enumerateDatumsH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Input client.Input `json:"input"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if err := validateInputSides(&body.Input, ""); err != nil {
		return err
	}
	out, err := d.enumerateInputDatums(&body.Input)
	if err != nil {
		return err
	}
	writeJSON(w, out)
	return nil
}

// enumerateInputDatums lists an input's datum set: the cartesian product
// of its sides' glob matches at their current finished heads. A side with
// no finished head contributes nothing, so the set is empty.
func (d *daemon) enumerateInputDatums(in *client.Input) ([]client.Datum, error) {
	sides := inputSides(in)
	sideLists := make([][]datumSide, len(sides))
	for i, s := range sides {
		head, err := d.store.headCommitRec(s.Repo, inputBranch(s))
		if err != nil || !head.Finished {
			continue
		}
		view, err := d.store.resolveViewByID(head.ID)
		if err != nil {
			return nil, err
		}
		name := s.Name
		if name == "" {
			name = s.Repo
		}
		sd := enumerateDatums(view, s.Glob)
		for j := range sd {
			sd[j].Name = name
		}
		sideLists[i] = sd
	}
	datums := crossDatums(sideLists)
	out := make([]client.Datum, 0, len(datums))
	for _, dt := range datums {
		dd := client.Datum{ID: dt.ID}
		for _, sd := range dt.Sides {
			for _, f := range sd.Files {
				dd.Files = append(dd.Files, client.DatumFile{
					Name: sd.Name,
					Path: f,
					Hash: "",
				})
			}
		}
		out = append(out, dd)
	}
	return out, nil
}

func (d *daemon) listFilesH(w http.ResponseWriter, r *http.Request) error {
	files, err := d.store.listFiles(r.PathValue("id"))
	if err != nil {
		return err
	}
	// a prefix-glob filter on the listing (SB-047 clause 4): "1*" lists
	// the paths beginning with "1". Any other pattern returns an error.
	if glob := r.URL.Query().Get("glob"); glob != "" {
		prefix, star, ok := strings.Cut(glob, "*")
		if !ok || strings.Contains(star, "*") {
			return fmt.Errorf("unsupported listing glob %q (prefix patterns only)", glob)
		}
		var out []client.FileInfo
		for _, f := range files {
			if strings.HasPrefix(f.Path, prefix) {
				out = append(out, f)
			}
		}
		writeJSON(w, out)
		return nil
	}
	writeJSON(w, files)
	return nil
}

// ---- pipelines ----

func (d *daemon) createPipelineH(w http.ResponseWriter, r *http.Request) error {
	var p pipelineRec
	if err := decodeBody(r, &p.Pipeline); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if tx := r.URL.Query().Get("transaction"); tx != "" {
		// stage the create/update into an open transaction (SB-162/163)
		if err := d.stageTxOp(tx, p.Pipeline); err != nil {
			return err
		}
		writeJSON(w, map[string]string{"ok": "true", "transaction": tx})
		return nil
	}
	if err := d.createPipeline(p.Pipeline); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) listPipelinesH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	var history *int
	if v := q.Get("history"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			history = &n
		}
	}
	pipes, err := d.listPipelinesFiltered(history, q.Get("name"), q.Get("allowIncomplete") == "1")
	if err != nil {
		return err
	}
	writeJSON(w, pipes)
	return nil
}

// triggerCronH is the manual cron trigger (SB-089 clauses 4-6).
func (d *daemon) triggerCronH(w http.ResponseWriter, r *http.Request) error {
	if err := d.triggerCron(r.PathValue("name")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) inspectPipelineH(w http.ResponseWriter, r *http.Request) error {
	ancestry, _ := strconv.Atoi(r.URL.Query().Get("ancestry"))
	info, err := d.inspectPipeline(r.PathValue("name"), ancestry)
	if err != nil {
		return err
	}
	writeJSON(w, info)
	return nil
}

func (d *daemon) deletePipelineH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	if err := d.deletePipeline(r.PathValue("name"), q.Get("force") == "1", q.Get("keepRepo") == "1"); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) stopPipelineH(w http.ResponseWriter, r *http.Request) error {
	if err := d.stopPipeline(r.PathValue("name")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) startPipelineH(w http.ResponseWriter, r *http.Request) error {
	if err := d.startPipeline(r.PathValue("name")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// runPipelineH handles a manual pipeline run (SB-010): a new job over
// explicit provenance commits (or the current heads), never propagating
// downstream.
func (d *daemon) runPipelineH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Provenance []string `json:"provenance"`
		Job        string   `json:"job"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	job, err := d.runPipeline(r.PathValue("name"), body.Provenance, body.Job)
	if err != nil {
		return err
	}
	writeJSON(w, job)
	return nil
}

// ---- jobs ----

// listDatumsH serves GET /api/v1/jobs/{id}/datums (SB-080/083).
func (d *daemon) listDatumsH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	page, _ := strconv.Atoi(q.Get("page"))
	out, err := d.listDatums(r.PathValue("id"), limit, page)
	if err != nil {
		return err
	}
	writeJSON(w, out)
	return nil
}

// inspectDatumH serves GET /api/v1/jobs/{id}/datums/{datumID} (SB-080).
func (d *daemon) inspectDatumH(w http.ResponseWriter, r *http.Request) error {
	info, err := d.inspectDatum(r.PathValue("id"), r.PathValue("datumID"))
	if err != nil {
		return err
	}
	writeJSON(w, info)
	return nil
}

// restartDatumH serves POST /api/v1/jobs/{id}/datums/{datumID}/restart
// (SB-064).
func (d *daemon) restartDatumH(w http.ResponseWriter, r *http.Request) error {
	if err := d.restartDatum(r.PathValue("id"), r.PathValue("datumID")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) listJobsH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	var history *int
	if v := q.Get("history"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			history = &n
		}
	}
	jobs, err := d.listJobsFiltered(q.Get("pipeline"), q.Get("outputCommit"), q["state"], q.Get("full") == "1", history, q["inputCommit"])
	if err != nil {
		return err
	}
	writeJSON(w, jobs)
	return nil
}

func (d *daemon) resetH(w http.ResponseWriter, r *http.Request) error {
	if err := d.reset(); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) inspectJobH(w http.ResponseWriter, r *http.Request) error {
	j, err := d.inspectJob(r.PathValue("id"))
	if err != nil {
		return err
	}
	writeJSON(w, j)
	return nil
}

func (d *daemon) cancelJobH(w http.ResponseWriter, r *http.Request) error {
	if err := d.cancelJob(r.PathValue("id")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) deleteJobH(w http.ResponseWriter, r *http.Request) error {
	if err := d.deleteJob(r.PathValue("id")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- tags (SB-150) ----

func (d *daemon) putTagH(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<30))
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}
	if err := d.store.putTag(r.PathValue("name"), data); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) getTagH(w http.ResponseWriter, r *http.Request) error {
	data, err := d.store.getTag(r.PathValue("name"))
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
	return nil
}

func (d *daemon) listTagsH(w http.ResponseWriter, r *http.Request) error {
	tags, err := d.store.listTags()
	if err != nil {
		return err
	}
	writeJSON(w, tags)
	return nil
}

func (d *daemon) deleteTagH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.deleteTag(r.PathValue("name")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}
