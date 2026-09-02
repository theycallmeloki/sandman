package main

// The HTTP API surface (/api/v1/...) lives on the same TCP port as the
// fabric's text protocol. The accept loop peeks the first bytes of each
// connection: an HTTP method line is handed to the API server, anything
// else (HELLO…) to the text protocol. One port, one discovery story, two
// protocols — each connection routed by its own shape.
//
// Every data-plane API endpoint validates its request before dereferencing
// fields: calls with missing required fields (empty names, unknown refs,
// empty paths) and malformed bodies are rejected with a well-formed error
// response rather than panicking. No handler may drop the connection on
// bad input — validation lives at the API boundary so the service survives
// arbitrary malformed requests.

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"sandman/client"
	"sandman/internal/store"
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

// chanListener delivers pre-routed connections to http.Server. The
// routing channel is owned by the accept path, so Close cannot close it
// (a send would panic); instead a done channel unblocks Accept with
// net.ErrClosed, letting http.Server.Shutdown complete (its listener
// WaitGroup counts the Serve goroutine, which only exits when Accept
// returns).
type chanListener struct {
	ch   chan net.Conn
	done chan struct{}
	once sync.Once
}

func (l *chanListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *chanListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
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
	routeConn(c, apiConns, d.text.serve)
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
	mux.HandleFunc("POST /api/v1/repos", d.instrument("repos.create", hErr(d.createRepoH)))
	mux.HandleFunc("GET /api/v1/repos", d.instrument("repos.list", hErr(d.listReposH)))
	mux.HandleFunc("GET /api/v1/repos/{name}", d.instrument("repos.inspect", hErr(d.inspectRepoH)))
	mux.HandleFunc("DELETE /api/v1/repos/{name}", d.instrument("repos.delete", hErr(d.deleteRepoH)))
	mux.HandleFunc("POST /api/v1/repos/{name}/commits", d.instrument("commits.start", hErr(d.startCommitH)))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches/{branch}/head", d.instrument("commits.head", hErr(d.headCommitH)))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches", d.instrument("branches.list", hErr(d.listBranchesH)))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches/{branch}", d.instrument("branches.inspect", hErr(d.inspectBranchH)))
	mux.HandleFunc("DELETE /api/v1/repos/{name}/branches/{branch}", d.instrument("branches.delete", hErr(d.deleteBranchH)))
	mux.HandleFunc("POST /api/v1/repos/{name}/branches/{branch}", d.instrument("branches.create", hErr(d.createBranchH)))
	mux.HandleFunc("POST /api/v1/commits/{id}/finish", d.instrument("commits.finish", hErr(d.finishCommitH)))
	mux.HandleFunc("GET /api/v1/commits/{id}", d.instrument("commits.inspect", hErr(d.inspectCommitH)))
	mux.HandleFunc("PUT /api/v1/commits/{id}/files/{path...}", d.instrument("files.put", hErr(d.putFileH)))
	mux.HandleFunc("POST /api/v1/commits/{id}/files/{path...}", d.instrument("files.copy", hErr(d.copyFileH)))
	mux.HandleFunc("DELETE /api/v1/commits/{id}/files/{path...}", d.instrument("files.delete", hErr(d.deleteFileH)))
	mux.HandleFunc("GET /api/v1/commits/{id}/files/{path...}", d.instrument("files.get", hErr(d.getFileH)))
	mux.HandleFunc("GET /api/v1/commits/{id}/files", d.instrument("files.list", hErr(d.listFilesH)))
	mux.HandleFunc("DELETE /api/v1/commits/{id}", d.instrument("commits.delete", hErr(d.deleteCommitH)))
	mux.HandleFunc("POST /api/v1/secrets", d.instrument("secrets.create", hErr(d.createSecretH)))
	mux.HandleFunc("GET /api/v1/secrets", d.instrument("secrets.list", hErr(d.listSecretsH)))
	mux.HandleFunc("GET /api/v1/secrets/{name}", d.instrument("secrets.inspect", hErr(d.inspectSecretH)))
	mux.HandleFunc("DELETE /api/v1/secrets/{name}", d.instrument("secrets.delete", hErr(d.deleteSecretH)))
	mux.HandleFunc("POST /api/v1/hosts", d.instrument("hosts.register", hErr(d.registerHostH)))
	mux.HandleFunc("GET /api/v1/hosts", d.instrument("hosts.list", hErr(d.listHostsH)))
	mux.HandleFunc("DELETE /api/v1/hosts/{name}", d.instrument("hosts.delete", hErr(d.deleteHostH)))
	mux.HandleFunc("GET /api/v1/metrics", hErr(d.metricsH))
	mux.HandleFunc("GET /api/v1/version", d.instrument("version.get", hErr(d.versionH)))
	mux.HandleFunc("GET /api/v1/backup", d.instrument("backup", hErr(d.backupH)))
	mux.HandleFunc("POST /api/v1/gc", d.instrument("gc", hErr(d.collectGarbageH)))
	mux.HandleFunc("POST /api/v1/check", d.instrument("check", hErr(d.checkH)))
	mux.HandleFunc("POST /api/v1/pipelines", d.instrument("pipelines.create", hErr(d.createPipelineH)))
	mux.HandleFunc("GET /api/v1/pipelines", d.instrument("pipelines.list", hErr(d.listPipelinesH)))
	mux.HandleFunc("GET /api/v1/pipelines/{name}", d.instrument("pipelines.inspect", hErr(d.inspectPipelineH)))
	mux.HandleFunc("DELETE /api/v1/pipelines/{name}", d.instrument("pipelines.delete", hErr(d.deletePipelineH)))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/stop", d.instrument("pipelines.stop", hErr(d.stopPipelineH)))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/start", d.instrument("pipelines.start", hErr(d.startPipelineH)))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/run", d.instrument("pipelines.run", hErr(d.runPipelineH)))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/trigger", d.instrument("pipelines.trigger", hErr(d.triggerCronH)))
	mux.HandleFunc("GET /api/v1/jobs", d.instrument("jobs.list", hErr(d.listJobsH)))
	mux.HandleFunc("GET /api/v1/jobs/{id}", d.instrument("jobs.inspect", hErr(d.inspectJobH)))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums", d.instrument("jobs.datums", hErr(d.listDatumsH)))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums/{datumID}", d.instrument("jobs.datum", hErr(d.inspectDatumH)))
	mux.HandleFunc("POST /api/v1/jobs/{id}/datums/{datumID}/restart", d.instrument("jobs.restart", hErr(d.restartDatumH)))
	mux.HandleFunc("POST /api/v1/git/push", d.instrument("git.push", hErr(d.gitPushH)))
	mux.HandleFunc("POST /api/v1/git/delta", d.instrument("git.delta", hErr(d.gitDeltaH)))
	mux.HandleFunc("POST /api/v1/flush", d.instrument("flush", hErr(d.flushH)))
	mux.HandleFunc("GET /api/v1/jobs/{id}/wait", d.instrument("jobs.wait", hErr(d.jobWaitH)))
	mux.HandleFunc("GET /api/v1/services", d.instrument("services.list", hErr(d.listServicesH)))
	mux.HandleFunc("GET /api/v1/services/{name}", d.instrument("services.inspect", hErr(d.inspectServiceH)))
	mux.HandleFunc("GET /api/v1/services/{pipeline}/{path...}", d.instrument("services.proxy", hErr(d.serviceProxyH)))
	mux.HandleFunc("GET /api/v1/logs", d.instrument("logs.get", hErr(d.logsH)))
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", d.instrument("jobs.cancel", hErr(d.cancelJobH)))
	mux.HandleFunc("POST /api/v1/jobs/{id}/stop", d.instrument("jobs.stop", hErr(d.cancelJobH)))
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", d.instrument("jobs.delete", hErr(d.deleteJobH)))
	mux.HandleFunc("POST /api/v1/transactions", d.instrument("transactions.start", hErr(d.startTransactionH)))
	mux.HandleFunc("GET /api/v1/transactions", d.instrument("transactions.list", hErr(d.listTransactionsH)))
	mux.HandleFunc("GET /api/v1/transactions/{id}", d.instrument("transactions.inspect", hErr(d.inspectTransactionH)))
	mux.HandleFunc("POST /api/v1/transactions/{id}/finish", d.instrument("transactions.finish", hErr(d.finishTransactionH)))
	mux.HandleFunc("DELETE /api/v1/transactions/{id}", d.instrument("transactions.delete", hErr(d.deleteTransactionH)))
	mux.HandleFunc("POST /api/v1/datums", d.instrument("datums.enumerate", hErr(d.enumerateDatumsH)))
	mux.HandleFunc("POST /api/v1/reset", d.instrument("reset", hErr(d.resetH)))
	mux.HandleFunc("PUT /api/v1/tags/{name}", d.instrument("tags.put", hErr(d.putTagH)))
	mux.HandleFunc("GET /api/v1/tags/{name}", d.instrument("tags.get", hErr(d.getTagH)))
	mux.HandleFunc("DELETE /api/v1/tags/{name}", d.instrument("tags.delete", hErr(d.deleteTagH)))
	mux.HandleFunc("GET /api/v1/tags", d.instrument("tags.list", hErr(d.listTagsH)))
	// the embedded read-only dashboard: index at /, assets under /ui/.
	// The API owns every /api/v1/... path; Go's ServeMux prefers the
	// more specific registered patterns, so these never shadow it, and
	// the bare catch-all below still 404s everything else (methods,
	// unknown paths) with the uniform JSON error shape. "GET /{$}" is
	// the exact-match anchor: without {$}, a method-qualified "/"
	// matches every path and would swallow the catch-all.
	mux.HandleFunc("GET /{$}", hErr(d.webIndexH))
	mux.HandleFunc("GET /ui/{path...}", hErr(d.webAssetH))
	// unknown paths get the uniform JSON error shape (mux's default
	// text/plain "404 page not found" would break the client's error
	// decode — and version-skew callers hitting a not-yet-existing
	// endpoint most need a readable message)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErr(w, http.StatusNotFound, "no such endpoint")
	})
	return mux
}

// hErr wraps a handler that returns a client error.
type handler func(w http.ResponseWriter, r *http.Request) error

// readBody reads up to maxBody bytes and rejects larger bodies instead
// of silently truncating them: a truncated upload would be stored as if
// it were the whole file.
func readBody(r io.Reader, maxBody int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBody {
		return nil, fmt.Errorf("body exceeds %d bytes", maxBody)
	}
	return data, nil
}

func hErr(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := h(w, r); err != nil {
			code := http.StatusBadRequest
			switch {
			case errors.Is(err, errNotFound) || errors.Is(err, store.ErrNotFound):
				code = http.StatusNotFound
			case errors.Is(err, errInternal):
				code = http.StatusInternalServerError
			}
			writeErr(w, code, err.Error())
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
	if r.ContentLength == 0 {
		// an empty body is an empty document: POST /commits/{id}/finish
		// with no body is a legitimate "no description" request, and
		// treating it as a parse error rejects otherwise-fine HTTP
		// clients that omit -d
		return nil
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<30)).Decode(v); err != nil {
		if err == io.EOF {
			return nil // a chunked request with no bytes is empty too
		}
		return err
	}
	return nil
}

// ---- repos ----

func (d *daemon) createRepoH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Name string `json:"name"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if err := d.store.CreateRepo(body.Name); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"name": body.Name})
	return nil
}

func (d *daemon) listReposH(w http.ResponseWriter, r *http.Request) error {
	repos, err := d.store.ListRepos()
	if err != nil {
		return err
	}
	writeJSON(w, repos)
	return nil
}

func (d *daemon) inspectRepoH(w http.ResponseWriter, r *http.Request) error {
	repo, err := d.store.InspectRepo(r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, repo)
	return nil
}

func (d *daemon) deleteRepoH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.DeleteRepo(r.PathValue("name"), r.URL.Query().Get("force") == "1"); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
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
	cm, err := d.store.StartCommit(r.PathValue("name"), body.Branch, body.Description)
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	return nil
}

func (d *daemon) headCommitH(w http.ResponseWriter, r *http.Request) error {
	cm, err := d.store.HeadCommitRec(r.PathValue("name"), r.PathValue("branch"))
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	return nil
}

// createBranchH points a branch at an existing commit, creating the branch
// or retargeting it.
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
	bs, err := d.store.BranchRefs(r.PathValue("name"))
	if err != nil {
		return err
	}
	writeJSON(w, bs)
	return nil
}

// inspectBranchH returns the named branch's head commit id.
func (d *daemon) inspectBranchH(w http.ResponseWriter, r *http.Request) error {
	head, err := d.store.BranchHead(r.PathValue("name"), r.PathValue("branch"))
	if err != nil {
		return err
	}
	writeJSON(w, client.Branch{Repo: r.PathValue("name"), Branch: r.PathValue("branch"), Head: head})
	return nil
}

// deleteBranchH removes the branch ref (the default branch is protected).
func (d *daemon) deleteBranchH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.DeleteBranch(r.PathValue("name"), r.PathValue("branch")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// createBranch points a branch at an existing commit — creating the branch
// or retargeting it. Pipelines watch branch heads, so the retarget is
// itself a trigger: the commit is now on the watched branch and is
// processed exactly once.
func (d *daemon) createBranch(repo, branch, head string) error {
	if branch == "" {
		return fmt.Errorf("branch must specify a name")
	}
	rec, err := d.store.LoadCommitByID(head)
	if err != nil {
		return notFound("commit %q not found", head)
	}
	if rec.Repo != repo {
		return fmt.Errorf("commit %s is not in repo %q", head, repo)
	}
	if err := d.store.SetHead(repo, branch, head); err != nil {
		return err
	}
	cm := rec.Commit()
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
	cm, err := d.store.FinishCommit(r.PathValue("id"), body.Description, body.Empty)
	if err != nil {
		return err
	}
	writeJSON(w, cm)
	d.triggerForCommit(cm)
	return nil
}

func (d *daemon) inspectCommitH(w http.ResponseWriter, r *http.Request) error {
	cm, err := d.store.InspectCommit(r.PathValue("id"))
	if err != nil {
		return err
	}
	// ?brief=1 omits the subvenant scan: a chain-walker (CommitHistory)
	// inspects every ancestor and never reads Subvenants, and the scan is
	// O(all commits) per inspection — the unconditional form would make a
	// commit list O(depth x total commits) of server work.
	if r.URL.Query().Get("brief") == "1" {
		writeJSON(w, cm)
		return nil
	}
	// Subvenants are the commits that derive from this one: every commit
	// whose recorded provenance includes it (a spec commit's
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
// everything derived from it across the DAG.
func (d *daemon) deleteCommitH(w http.ResponseWriter, r *http.Request) error {
	if err := d.deleteCommit(r.PathValue("id")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- runtime metrics ----

// latencyBuckets are the histogram upper bounds (seconds) for every
// latency series; +Inf is implied. Fine-grained at the low end because
// control-plane operations are mostly sub-millisecond local file/API
// calls.
var latencyBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// hist is a Prometheus-style histogram: sum/count plus cumulative bucket
// counts (buckets[i] = observations <= latencyBuckets[i]). A nil buckets
// slice means no observations yet.
type hist struct {
	sum     float64
	count   int64
	buckets []uint64
}

// observe records one latency sample into the histogram.
func (h *hist) observe(dur float64) {
	h.sum += dur
	h.count++
	if h.buckets == nil {
		h.buckets = make([]uint64, len(latencyBuckets))
	}
	for i, ub := range latencyBuckets {
		if dur <= ub {
			h.buckets[i]++
			break
		}
	}
}

// add returns a histogram with the sums, counts, and bucket counts of two
// histograms combined (used to merge a per-op success/error split back
// into one series).
func (a hist) add(b hist) hist {
	out := hist{sum: a.sum + b.sum, count: a.count + b.count}
	if a.buckets != nil || b.buckets != nil {
		out.buckets = make([]uint64, len(latencyBuckets))
		for i := range out.buckets {
			if a.buckets != nil {
				out.buckets[i] += a.buckets[i]
			}
			if b.buckets != nil {
				out.buckets[i] += b.buckets[i]
			}
		}
	}
	return out
}

// metricsStore accumulates the instrumented operations' invocation counts
// and latency aggregates. File-read latency is split by outcome.
type metricsStore struct {
	mu         sync.Mutex
	readTotal  int64
	readOK     hist // successful file reads
	readErr    hist // errored file reads
	write      hist
	writeTotal int64
	list       hist
	listTotal  int64
	// ops accumulates per-API-verb invocation counters and latency
	// aggregates for every instrumented route (repos.list, jobs.inspect,
	// pipelines.create, ...); ok/err split by response status.
	ops map[string]*opMetrics
}

type opMetrics struct {
	total int64
	ok    hist
	err   hist
}

func (m *metricsStore) observe(op string, dur float64, err bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// the three legacy families stay emitted for compatibility; every
	// op also lands in the generic per-verb series
	switch op {
	case "files.get":
		m.readTotal++
		if err {
			m.readErr.observe(dur)
		} else {
			m.readOK.observe(dur)
		}
	case "files.put":
		m.writeTotal++
		m.write.observe(dur)
	case "jobs.list":
		m.listTotal++
		m.list.observe(dur)
	}
	om := m.ops[op]
	if om == nil {
		om = &opMetrics{}
		if m.ops == nil {
			m.ops = make(map[string]*opMetrics)
		}
		m.ops[op] = om
	}
	om.total++
	if err {
		om.err.observe(dur)
	} else {
		om.ok.observe(dur)
	}
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

// Flush forwards to the underlying writer so streaming handlers (logs
// follow) keep their flush capability. Embedding the ResponseWriter
// interface alone drops it: w.(http.Flusher) fails on the wrapper, and
// followLogs returns immediately with an empty stream.
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// instrument wraps an HTTP handler with its operation's invocation counter
// and latency histogram.
func (d *daemon) instrument(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusRecorder{ResponseWriter: w, status: 200}
		h(sw, r)
		d.metrics.observe(op, time.Since(start).Seconds(), sw.status >= 400)
	}
}

// versionH reports the daemon's baked build version (the same Version the
// binary carries; a `sandman --version` shows both so a stale daemon is
// visible).
func (d *daemon) versionH(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, map[string]string{"version": Version})
	return nil
}

// metricsH renders the runtime metrics in standard Prometheus exposition
// format: monotone invocation counters and latency sum/count aggregates for
// file reads, file writes, and job listings. File-read latency is split by
// outcome into exactly two series (success and error), so an average is
// computable even when some operations errored; write and job-listing
// latency each yield exactly one series.
func (d *daemon) metricsH(w http.ResponseWriter, r *http.Request) error {
	d.metrics.mu.Lock()
	readTotal := d.metrics.readTotal
	readOK, readErr := d.metrics.readOK, d.metrics.readErr
	writeTotal, writeH := d.metrics.writeTotal, d.metrics.write
	listTotal, listH := d.metrics.listTotal, d.metrics.list
	ops := make([]string, 0, len(d.metrics.ops))
	for op := range d.metrics.ops {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	opsCopy := make(map[string]opMetrics, len(d.metrics.ops))
	for op, om := range d.metrics.ops {
		opsCopy[op] = *om
	}
	d.metrics.mu.Unlock()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# HELP sandman_file_read_total File read invocations.\n# TYPE sandman_file_read_total counter\nsandman_file_read_total %d\n", readTotal)
	fmt.Fprintf(w, "# TYPE sandman_file_read_seconds histogram\n")
	emitHist(w, "sandman_file_read_seconds", `outcome="success"`, readOK)
	emitHist(w, "sandman_file_read_seconds", `outcome="error"`, readErr)
	fmt.Fprintf(w, "# HELP sandman_file_write_total File write invocations.\n# TYPE sandman_file_write_total counter\nsandman_file_write_total %d\n", writeTotal)
	fmt.Fprintf(w, "# TYPE sandman_file_write_seconds histogram\n")
	emitHist(w, "sandman_file_write_seconds", "", writeH)
	fmt.Fprintf(w, "# HELP sandman_job_list_total Job listing invocations.\n# TYPE sandman_job_list_total counter\nsandman_job_list_total %d\n", listTotal)
	fmt.Fprintf(w, "# TYPE sandman_job_list_seconds histogram\n")
	emitHist(w, "sandman_job_list_seconds", "", listH)
	// per-verb API series: one counter, one latency sum/count pair, and
	// one error counter per instrumented route (repos.list, jobs.inspect,
	// pipelines.create, ...)
	fmt.Fprintf(w, "# HELP sandman_api_requests_total API invocations by verb.\n# TYPE sandman_api_requests_total counter\n")
	fmt.Fprintf(w, "# HELP sandman_api_request_seconds API latency by verb.\n# TYPE sandman_api_request_seconds histogram\n")
	fmt.Fprintf(w, "# HELP sandman_api_request_errors_total API invocations returning >=400 by verb.\n# TYPE sandman_api_request_errors_total counter\n")
	for _, op := range ops {
		om := opsCopy[op]
		fmt.Fprintf(w, "sandman_api_requests_total{op=%q} %d\n", op, om.total)
		emitHist(w, "sandman_api_request_seconds", fmt.Sprintf("op=%q", op), om.ok.add(om.err))
		fmt.Fprintf(w, "sandman_api_request_errors_total{op=%q} %d\n", op, om.err.count)
	}
	// fleet state gauges (TTL-cached)
	jobs, pipes, hosts, hostsGPU, gpus, spouts := d.fleetCounts()
	fmt.Fprintf(w, "# HELP sandman_hosts_total Registered execution hosts.\n# TYPE sandman_hosts_total gauge\nsandman_hosts_total %d\n", hosts)
	fmt.Fprintf(w, "# HELP sandman_hosts_with_gpus_total Registered hosts advertising GPUs.\n# TYPE sandman_hosts_with_gpus_total gauge\nsandman_hosts_with_gpus_total %d\n", hostsGPU)
	fmt.Fprintf(w, "# HELP sandman_gpus_total GPUs advertised across the fleet.\n# TYPE sandman_gpus_total gauge\nsandman_gpus_total %d\n", gpus)
	fmt.Fprintf(w, "# HELP sandman_spouts_total Spout pipelines.\n# TYPE sandman_spouts_total gauge\nsandman_spouts_total %d\n", spouts)
	// every known state is emitted (0-filled) so dashboards never lose a
	// series when a state empties; absent states would otherwise vanish
	// from scrapes and read as "no data" instead of zero
	fmt.Fprintf(w, "# HELP sandman_jobs Jobs by state.\n# TYPE sandman_jobs gauge\n")
	for _, st := range []string{stateRunning, stateQueued, stateFailure, stateCrashed, stateSuccess} {
		fmt.Fprintf(w, "sandman_jobs{state=%q} %d\n", st, jobs[st])
	}
	fmt.Fprintf(w, "# HELP sandman_pipelines Pipelines by state.\n# TYPE sandman_pipelines gauge\n")
	for _, st := range []string{stateRunning, stateFailure, stateCrashed, "stopped"} {
		fmt.Fprintf(w, "sandman_pipelines{state=%q} %d\n", st, pipes[st])
	}
	return nil
}

// emitHist renders one latency histogram's sum, count, and cumulative
// bucket series in Prometheus exposition format. labels is the
// comma-separated label list inside the braces ("" for a label-less
// series); the le="+Inf" bucket carries the count, matching the
// histogram convention so histogram_quantile works.
func emitHist(w io.Writer, name, labels string, h hist) {
	if labels == "" {
		fmt.Fprintf(w, "%s_sum %g\n", name, h.sum)
		fmt.Fprintf(w, "%s_count %d\n", name, h.count)
	} else {
		fmt.Fprintf(w, "%s_sum{%s} %g\n", name, labels, h.sum)
		fmt.Fprintf(w, "%s_count{%s} %d\n", name, labels, h.count)
	}
	for i, ub := range latencyBuckets {
		var b uint64
		if i < len(h.buckets) {
			b = h.buckets[i]
		}
		if labels == "" {
			fmt.Fprintf(w, "%s_bucket{le=%q} %d\n", name, fmt.Sprint(ub), b)
		} else {
			fmt.Fprintf(w, "%s_bucket{%s, le=%q} %d\n", name, labels, fmt.Sprint(ub), b)
		}
	}
	if labels == "" {
		fmt.Fprintf(w, "%s_bucket{le=\"+Inf\"} %d\n", name, h.count)
	} else {
		fmt.Fprintf(w, "%s_bucket{%s, le=\"+Inf\"} %d\n", name, labels, h.count)
	}
}

// countsCache holds the TTL-cached fleet gauges.
type countsCache struct {
	mu       sync.Mutex
	at       time.Time
	jobs     map[string]int64
	pipes    map[string]int64
	hosts    int64
	hostsGPU int64
	gpus     int64
	spout    int64
}

// fleetCounts computes (or returns cached) jobs by state, pipelines by
// state, registered hosts, hosts with GPUs, advertised GPUs, and spout
// pipelines. The state scan walks the jobs/pipelines directories, so it is
// cached for a few seconds between scrapes.
func (d *daemon) fleetCounts() (jobs, pipes map[string]int64, hosts, hostsGPU, gpus, spouts int64) {
	d.metricCounts.mu.Lock()
	defer d.metricCounts.mu.Unlock()
	if time.Since(d.metricCounts.at) < 5*time.Second && d.metricCounts.jobs != nil {
		return d.metricCounts.jobs, d.metricCounts.pipes, d.metricCounts.hosts, d.metricCounts.hostsGPU, d.metricCounts.gpus, d.metricCounts.spout
	}
	jobs = map[string]int64{}
	entries, err := os.ReadDir(filepath.Join(d.state, "jobs"))
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			b, err := os.ReadFile(filepath.Join(d.state, "jobs", e.Name(), "job.json"))
			if err != nil {
				continue
			}
			var rec jobRec
			if json.Unmarshal(b, &rec) != nil {
				continue
			}
			jobs[rec.State]++
		}
	}
	pipes = map[string]int64{}
	pents, err := os.ReadDir(filepath.Join(d.state, "pipelines"))
	if err == nil {
		for _, e := range pents {
			if !strings.HasSuffix(e.Name(), ".json") {
				continue
			}
			rec, err := d.loadPipeline(strings.TrimSuffix(e.Name(), ".json"))
			if err != nil {
				continue
			}
			st := rec.State
			if rec.Stopped {
				st = "stopped"
			}
			pipes[st]++
			if rec.Pipeline.Spout != nil {
				spouts++
			}
		}
	}
	for _, h := range d.hosts.list() {
		hosts++
		if len(h.Gpus) > 0 {
			hostsGPU++
		}
		gpus += int64(len(h.Gpus))
	}
	d.metricCounts.jobs, d.metricCounts.pipes = jobs, pipes
	d.metricCounts.hosts, d.metricCounts.hostsGPU, d.metricCounts.gpus, d.metricCounts.spout = hosts, hostsGPU, gpus, spouts
	d.metricCounts.at = time.Now()
	return
}

// ---- garbage collection ----

// checkH is the consistency check: every piece of control-plane metadata
// parses; a corrupted record is reported as an error, an intact system
// reports ok. A system-wide reset (POST /api/v1/reset) runs the same check
// first, because a full reset requires healthy metadata — corrupted
// metadata is an error, not tolerated.
func (d *daemon) checkH(w http.ResponseWriter, r *http.Request) error {
	if err := d.checkMetadata(); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// collectGarbageH is the manual collection trigger: reclamation may run
// automatically or be triggered manually, and automatic collection
// defaults off.
func (d *daemon) collectGarbageH(w http.ResponseWriter, r *http.Request) error {
	if err := d.collectGarbage(); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// collectGarbage reclaims exactly the durable artifacts no longer
// referenced by any commit tree, tag, or spec record. It refuses while any
// job is running — active processing may still be about to read the data.
// Reachable data is never touched: deleting a pipeline reclaims its output
// repo's unreferenced blobs while content-deduplicated shared blobs and
// the pipeline's retained spec revisions survive. After collection,
// re-creating the same pipeline and input must yield fully readable,
// correct data — no stale cache may resurface collected storage.
func (d *daemon) collectGarbage() error {
	for _, j := range d.mustListJobs() {
		if j.State == stateRunning || j.State == stateQueued {
			// name the offender: a GC refusal on a stale "running" record
			// from a dead/cancelled job is a bug in whoever left it, and
			// the job id + pipeline make the culprit identifiable in one
			// log line
			return fmt.Errorf("cannot collect garbage while job %s (pipeline %s) is running", j.ID, j.Pipeline)
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
	// tags hold a reference to their blob, so collection never reclaims
	// reachable tagged data
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

// ---- secrets ----

// secretRec is a secret's durable record: a named metadata blob with a
// type label and key/value data — durable, like every other meta-plane
// record.
type secretRec struct {
	Name    string            `json:"name"`
	Type    string            `json:"type"`
	Created string            `json:"created"`
	Data    map[string]string `json:"data,omitempty"`
}

func (d *daemon) secretPath(name string) string {
	return filepath.Join(d.state, "secrets", name+".json")
}

// createSecretH stores a named secret: a metadata blob carrying key/value
// data and a type label, supporting create, inspect, list, and delete
// through the management API. Inspection reports the name, the type
// (arbitrary JSON data is reported as "Opaque"), and a system-assigned
// creation timestamp. After deletion the secret no longer appears in
// listings and inspection errors, and deleting an already-removed secret
// is a no-op (idempotent in effect).
func (d *daemon) createSecretH(w http.ResponseWriter, r *http.Request) error {
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
	if !store.ValidName(body.Name) {
		return fmt.Errorf("invalid secret name %q", body.Name)
	}
	rec := secretRec{
		Name:    body.Name,
		Type:    "Opaque",
		Created: now(),
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
	name := r.PathValue("name")
	if !store.ValidName(name) {
		return fmt.Errorf("invalid secret name %q", name)
	}
	rec, err := d.loadSecret(name)
	if err != nil {
		return err
	}
	writeJSON(w, client.SecretInfo{Name: rec.Name, Type: rec.Type, Created: rec.Created})
	return nil
}

func (d *daemon) loadSecret(name string) (*secretRec, error) {
	b, err := os.ReadFile(d.secretPath(name))
	if err != nil {
		return nil, notFound("secret %q not found", name)
	}
	var rec secretRec
	if json.Unmarshal(b, &rec) != nil {
		return nil, fmt.Errorf("secret %q is corrupt", name)
	}
	return &rec, nil
}

func (d *daemon) listSecretsH(w http.ResponseWriter, r *http.Request) error {
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
	name := r.PathValue("name")
	if !store.ValidName(name) {
		return fmt.Errorf("invalid secret name %q", name)
	}
	os.Remove(d.secretPath(name)) // idempotent in effect
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- execution hosts ----

// registerHostH is the join endpoint an execution host calls at setup and
// on its heartbeat: the worker reports its name, its exec endpoint, and
// the placement labels it bears. An operator designates execution hosts
// with placement labels, and a pipeline may require that its work run on a
// host bearing a specific label; the control plane schedules labeled work
// onto a registered host bearing the label, and a pipeline definition
// never enumerates a host address or identity. A job completes only once a
// host bearing the required label is available, and its output provably
// came from that host's execution.
func (d *daemon) registerHostH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Name   string    `json:"name"`
		Addr   string    `json:"addr"`
		Labels []string  `json:"labels"`
		Gpus   []GpuInfo `json:"gpus"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if body.Name == "" || body.Addr == "" {
		return fmt.Errorf("host registration needs a name and an address")
	}
	h := d.hosts.register(body.Name, body.Addr, body.Labels, body.Gpus)
	writeJSON(w, h)
	return nil
}

func (d *daemon) listHostsH(w http.ResponseWriter, r *http.Request) error {
	writeJSON(w, d.hosts.list())
	return nil
}

func (d *daemon) deleteHostH(w http.ResponseWriter, r *http.Request) error {
	d.hosts.drop(r.PathValue("name"))
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// ---- files ----

// putFileH stores a file in an open commit; the request body becomes the
// file's content and ?overwrite=1 replaces accumulated content at the
// path. With fetch=URL the file is ingested from an HTTP(S) URL: the
// URL's body becomes the content, redirects are not followed, and
// link-local/broadcast/metadata ranges are rejected (loopback stays
// allowed). With split=1&delimiter=X[&header=1] the upload is split into
// records stored at path/<index>; a header chunk is replicated into every
// record's file so each processing participant sees it exactly once, and
// appending records under the same header leaves earlier records'
// identity unchanged (they are skipped by the dedup, never reprocessed).
// A changed header re-identifies every existing record, rewriting it with
// the new header while keeping its path and record content, so all are
// reprocessed — none skipped.
func (d *daemon) putFileH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	// fetch=URL: ingest a remote file — the URL's body becomes
	// the file's content. The fetch is a server-side request to a
	// caller-chosen URL, so it is constrained: http(s) only, link-local
	// and broadcast destinations rejected (cloud-metadata ranges like
	// 169.254.169.254 must not be reachable through the daemon), redirects
	// not followed, bounded by the request context and a client timeout.
	// Loopback stays allowed: the documented ingest story includes local
	// HTTP servers (the conformance suite's own URL-ingest fixture).
	if u := q.Get("fetch"); u != "" {
		parsed, err := url.Parse(u)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			return fmt.Errorf("fetch %s: only http(s) URLs are allowed", u)
		}
		// The host is resolved now and every address checked, so a
		// hostname resolving to the forbidden ranges is rejected too, not
		// just a literal IP (a DNS-rebinding URL must not reach cloud
		// metadata like 169.254.169.254). The dial itself re-resolves, so
		// this is a gate for the common single-answer case, not a binding
		// guarantee.
		addrs, err := net.LookupIP(parsed.Hostname())
		if err != nil {
			return internal("fetch %s: %v", u, err)
		}
		for _, ip := range addrs {
			if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast() {
				return fmt.Errorf("fetch %s: link-local and broadcast addresses are not reachable", u)
			}
		}
		req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
		if err != nil {
			return internal("fetch %s: %v", u, err)
		}
		client := &http.Client{
			Timeout:       60 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
		resp, err := client.Do(req)
		if err != nil {
			return internal("fetch %s: %v", u, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 300 {
			return internal("fetch %s: status %d", u, resp.StatusCode)
		}
		data, err := readBody(resp.Body, 1<<30)
		if err != nil {
			return internal("read fetch: %v", err)
		}
		if err := d.store.PutFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
			return err
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	defer r.Body.Close()
	data, err := readBody(r.Body, 1<<30)
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}
	// split=1&delimiter=X[&header=1]: split the upload into records at
	// the delimiter; with a header, the first chunk is the header and is
	// replicated into every record's file, each stored at path/<i>
	// (same-header appends leave earlier records' identity unchanged, so
	// they are skipped by the dedup)
	if q.Get("split") == "1" {
		delim := q.Get("delimiter")
		header := q.Get("header") == "1"
		// a split with a header needs at least one delimiter occurrence:
		// the body-less slice below (chunks[1:]) panics otherwise
		if delim == "" {
			return fmt.Errorf("split upload needs a non-empty delimiter")
		}
		chunks := strings.Split(string(data), delim)
		if header && len(chunks) < 2 {
			return fmt.Errorf("split upload with a header needs at least one delimiter in the body")
		}
		start := 0
		if header {
			start = 1
		}
		records := chunks[start:]
		prefix := r.PathValue("path") + "/"
		base := 0
		var firstHeader string
		if view, err := d.store.ResolveViewByID(r.PathValue("id")); err == nil {
			for p := range view {
				if strings.HasPrefix(p, prefix) {
					base++
				}
			}
		}
		// a changed header re-identifies every record: the existing record
		// paths are overwritten with the new header + their existing record
		// content (a the header swaps everywhere without changing the
		// record count or numbering), so all are reprocessed
		changed := false
		if header && base > 0 {
			if first, err := d.store.GetFile(r.PathValue("id"), prefix+"0"); err == nil {
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
				stored, err := d.store.GetFile(r.PathValue("id"), prefix+strconv.Itoa(i))
				if err != nil {
					return err
				}
				rec := string(stored)
				if j := strings.Index(rec, delim); j >= 0 {
					rec = rec[j+len(delim):]
				}
				if err := d.store.OverwriteFile(r.PathValue("id"), prefix+strconv.Itoa(i), []byte(chunks[0]+delim+rec)); err != nil {
					return err
				}
			}
			records = records[min(base, len(records)):]
		}
		// new records continue the numbering after the existing records,
		// whether or not the header changed
		off := base
		for i, rec := range records {
			content := rec
			if header {
				content = chunks[0] + delim + content
			}
			if err := d.store.PutFile(r.PathValue("id"), prefix+strconv.Itoa(off+i), []byte(content)); err != nil {
				return err
			}
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	if q.Get("overwrite") == "1" {
		if err := d.store.OverwriteFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
			return err
		}
		writeJSON(w, map[string]string{"ok": "true"})
		return nil
	}
	if err := d.store.PutFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// getFileH serves a file from a commit by path: a plain GET returns the
// exact bytes with no Content-Disposition; a download=true GET returns the
// same bytes with attachment disposition naming the path's basename. The
// Content-Type is detected from the bytes (never a stored label), so
// binary files are served with a correct type and round-trip
// byte-for-byte. history=1 turns the read into a revision-history listing:
// one FileInfo per ancestor revision where the path resolves, newest
// first, capped by limit (negative = every revision)
func (d *daemon) getFileH(w http.ResponseWriter, r *http.Request) error {
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
	data, err := d.store.GetFile(r.PathValue("id"), r.PathValue("path"))
	if err != nil {
		return err
	}
	// Content type is detected from the bytes, not a stored label.
	w.Header().Set("Content-Type", http.DetectContentType(data))
	if r.URL.Query().Get("download") == "true" {
		// the basename is percent-decoded untrusted input: strip quotes,
		// backslashes and CR/LF so it cannot break out of the quoted
		// filename (net/http already rejects CR/LF in header values, but
		// the header must also stay well-formed for download clients)
		base := strings.NewReplacer(`"`, "", `\`, "", "\r", "", "\n", "").Replace(filepath.Base(r.PathValue("path")))
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, base))
	}
	_, _ = w.Write(data)
	return nil
}

// fileHistory lists the revisions of a path across the commit's ancestry:
// one FileInfo per ancestor revision where the path resolves, newest
// first, capped at limit (negative = every revision). It must not fail on
// outputs produced from multi-commit cross inputs — a cross's
// multi-commit provenance is just more ancestry to walk.
func (d *daemon) fileHistory(commitID, path string, limit int) ([]client.FileInfo, error) {
	rec, err := d.store.LoadCommitByID(commitID)
	if err != nil {
		return nil, err
	}
	var out []client.FileInfo
	for cur := rec; cur != nil; {
		if f, ok := d.store.ResolveView(cur)[path]; ok {
			h, err := f.Hash(d.store)
			if err != nil {
				return nil, err
			}
			out = append(out, client.FileInfo{Path: path, Size: f.Size(), Hash: h})
			if limit >= 0 && len(out) >= limit {
				break
			}
		}
		if cur.ParentID == "" {
			break
		}
		parent, err := d.store.LoadCommit(cur.Repo, cur.ParentID)
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
	if err := d.store.CopyFile(r.PathValue("id"), r.PathValue("path"), body.SrcCommit, body.SrcPath, overwrite); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) deleteFileH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.DeleteFile(r.PathValue("id"), r.PathValue("path")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

// enumerateDatumsH serves POST /api/v1/datums: the datum set an input
// would process at its sides' current heads, without creating or running a
// pipeline.
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

// enumerateInputDatums serves the datum set an input would process at its
// sides' current finished heads, without creating or running a pipeline:
// each side's glob matches are enumerated and combined as the Cartesian
// product, so two inputs of 5 files each yield 25 datums. A side with no
// finished head contributes nothing, so the set is empty.
func (d *daemon) enumerateInputDatums(in *client.Input) ([]client.Datum, error) {
	sides := inputSides(in)
	sideLists := make([][]datumSide, len(sides))
	for i, s := range sides {
		head, err := d.store.HeadCommitRec(s.Repo, inputBranch(s))
		if err != nil || !head.Finished {
			continue
		}
		view, err := d.store.ResolveViewByID(head.ID)
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

// listFilesH lists a commit's files. An optional prefix-glob filter
// (glob=1*) returns exactly the paths starting with the given prefix, and
// any pattern that is not a single 'prefix*' form is rejected as an
// unsupported listing glob. A single job must be able to land tens of
// thousands of files into one output commit, and the filtered counts must
// be exact across digit-length boundaries (over names 0-19999, 1*/5*/9*
// yield 11111/1111/1111).
func (d *daemon) listFilesH(w http.ResponseWriter, r *http.Request) error {
	files, err := d.store.ListFiles(r.PathValue("id"))
	if err != nil {
		return err
	}
	// a prefix-glob filter on the listing: "1*" lists
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
	// strict spec decode: an unknown field (a typo, or a ported spec
	// carrying an unsupported field like top-level resource_limits)
	// must fail the request loudly — silently ignoring it drops the
	// declaration with no error, a quiet resource-policy loss. The
	// error names the field (400).
	dec := json.NewDecoder(io.LimitReader(r.Body, 1<<30))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p.Pipeline); err != nil {
		r.Body.Close()
		return fmt.Errorf("invalid request body: %w", err)
	}
	r.Body.Close()
	if tx := r.URL.Query().Get("transaction"); tx != "" {
		// stage the create/update into an open transaction: the staged
		// operations apply atomically on finish — all or nothing — so a
		// pipeline staged here may consume another pipeline staged in the
		// same transaction (its output repo does not exist yet). Staging
		// records the pipeline's baseline version: if the same pipeline is
		// modified outside the transaction before finish, finish refuses
		// to commit rather than silently overwriting.
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

// triggerCronH is the manual cron trigger: it creates a tick immediately
// on every cron input of the pipeline regardless of schedule, and
// scheduled ticks keep flowing around it.
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

// runPipelineH handles a manual pipeline run: with no provenance the run
// re-processes the current branch heads, and with explicit provenance it
// processes exactly the requested input revisions (one per side, matched
// by the side's repo and branch), never the branch heads. Provenance
// commits outside the pipeline's input lineage, and two commits of the
// same branch, are rejected; a pipeline with no input commits and no
// provenance errors as unrunnable. The run's job carries the Manual flag
// and its output never propagates downstream — a manual run is not a
// processing wave. A job id re-executes an existing job's input pairing,
// adding a job rather than replacing it.
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

// listDatumsH serves GET /api/v1/jobs/{id}/datums.
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

// inspectDatumH serves GET /api/v1/jobs/{id}/datums/{datumID}.
func (d *daemon) inspectDatumH(w http.ResponseWriter, r *http.Request) error {
	info, err := d.inspectDatum(r.PathValue("id"), r.PathValue("datumID"))
	if err != nil {
		return err
	}
	writeJSON(w, info)
	return nil
}

// restartDatumH serves POST /api/v1/jobs/{id}/datums/{datumID}/restart:
// aborting the datum's in-flight processing and starting it over from
// scratch, with the next status observation showing it running with a
// strictly later start time.
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
	if r.URL.Query().Get("yes") != "1" {
		return fmt.Errorf("reset destroys every repo and pipeline; pass ?yes=1")
	}
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

// ---- tags ----

// putTagH binds a durable global name to file content: putting a tag
// stores the content's reference and getting it returns the exact bytes;
// listing enumerates every stored tag, each with a non-empty object
// reference. Tagged objects survive garbage collection — a tag holds a
// reference to its blob, so collection never reclaims reachable tagged
// data.
func (d *daemon) putTagH(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	data, err := readBody(r.Body, 1<<30)
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}
	if err := d.store.PutTag(r.PathValue("name"), data); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) getTagH(w http.ResponseWriter, r *http.Request) error {
	data, err := d.store.GetTag(r.PathValue("name"))
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(data)
	return nil
}

func (d *daemon) listTagsH(w http.ResponseWriter, r *http.Request) error {
	tags, err := d.store.ListTags()
	if err != nil {
		return err
	}
	writeJSON(w, tags)
	return nil
}

func (d *daemon) deleteTagH(w http.ResponseWriter, r *http.Request) error {
	if err := d.store.DeleteTag(r.PathValue("name")); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}
