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
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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
	d.handleConn(c, br)
}

func (d *daemon) apiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/repos", hErr(d.createRepoH))
	mux.HandleFunc("GET /api/v1/repos", hErr(d.listReposH))
	mux.HandleFunc("GET /api/v1/repos/{name}", hErr(d.inspectRepoH))
	mux.HandleFunc("DELETE /api/v1/repos/{name}", hErr(d.deleteRepoH))
	mux.HandleFunc("POST /api/v1/repos/{name}/commits", hErr(d.startCommitH))
	mux.HandleFunc("GET /api/v1/repos/{name}/branches/{branch}/head", hErr(d.headCommitH))
	mux.HandleFunc("POST /api/v1/commits/{id}/finish", hErr(d.finishCommitH))
	mux.HandleFunc("GET /api/v1/commits/{id}", hErr(d.inspectCommitH))
	mux.HandleFunc("PUT /api/v1/commits/{id}/files/{path...}", hErr(d.putFileH))
	mux.HandleFunc("POST /api/v1/commits/{id}/files/{path...}", hErr(d.copyFileH))
	mux.HandleFunc("GET /api/v1/commits/{id}/files/{path...}", hErr(d.getFileH))
	mux.HandleFunc("GET /api/v1/commits/{id}/files", hErr(d.listFilesH))
	mux.HandleFunc("POST /api/v1/pipelines", hErr(d.createPipelineH))
	mux.HandleFunc("GET /api/v1/pipelines", hErr(d.listPipelinesH))
	mux.HandleFunc("GET /api/v1/pipelines/{name}", hErr(d.inspectPipelineH))
	mux.HandleFunc("DELETE /api/v1/pipelines/{name}", hErr(d.deletePipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/stop", hErr(d.stopPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/start", hErr(d.startPipelineH))
	mux.HandleFunc("GET /api/v1/jobs", hErr(d.listJobsH))
	mux.HandleFunc("GET /api/v1/jobs/{id}", hErr(d.inspectJobH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", hErr(d.cancelJobH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/stop", hErr(d.cancelJobH))
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", hErr(d.deleteJobH))
	mux.HandleFunc("POST /api/v1/reset", hErr(d.resetH))
	mux.HandleFunc("PUT /api/v1/tags/{name}", hErr(d.putTagH))
	mux.HandleFunc("GET /api/v1/tags/{name}", hErr(d.getTagH))
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
	writeJSON(w, cm)
	return nil
}

// ---- files ----

func (d *daemon) putFileH(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, 1<<30))
	if err != nil {
		return fmt.Errorf("read body: %v", err)
	}
	if err := d.store.putFile(r.PathValue("id"), r.PathValue("path"), data); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) getFileH(w http.ResponseWriter, r *http.Request) error {
	data, err := d.store.getFile(r.PathValue("id"), r.PathValue("path"))
	if err != nil {
		return err
	}
	// Content type is detected from the bytes, not a stored label (SB-099).
	w.Header().Set("Content-Type", http.DetectContentType(data))
	if r.URL.Query().Get("download") == "true" {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filepath.Base(r.PathValue("path"))))
	}
	w.Write(data)
	return nil
}

func (d *daemon) copyFileH(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		SrcCommit string `json:"srcCommit"`
		SrcPath   string `json:"srcPath"`
	}
	if err := decodeBody(r, &body); err != nil {
		return fmt.Errorf("invalid request body")
	}
	if err := d.store.copyFile(r.PathValue("id"), r.PathValue("path"), body.SrcCommit, body.SrcPath); err != nil {
		return err
	}
	writeJSON(w, map[string]string{"ok": "true"})
	return nil
}

func (d *daemon) listFilesH(w http.ResponseWriter, r *http.Request) error {
	files, err := d.store.listFiles(r.PathValue("id"))
	if err != nil {
		return err
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

// ---- jobs ----

func (d *daemon) listJobsH(w http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	var history *int
	if v := q.Get("history"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			history = &n
		}
	}
	jobs, err := d.listJobsFiltered(q.Get("pipeline"), q.Get("outputCommit"), q["state"], q.Get("full") == "1", history)
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
	w.Write(data)
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
