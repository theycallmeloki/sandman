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
	mux.HandleFunc("POST /api/v1/repos/{name}/branches/{branch}", hErr(d.createBranchH))
	mux.HandleFunc("POST /api/v1/commits/{id}/finish", hErr(d.finishCommitH))
	mux.HandleFunc("GET /api/v1/commits/{id}", hErr(d.inspectCommitH))
	mux.HandleFunc("PUT /api/v1/commits/{id}/files/{path...}", hErr(d.putFileH))
	mux.HandleFunc("POST /api/v1/commits/{id}/files/{path...}", hErr(d.copyFileH))
	mux.HandleFunc("DELETE /api/v1/commits/{id}/files/{path...}", hErr(d.deleteFileH))
	mux.HandleFunc("GET /api/v1/commits/{id}/files/{path...}", hErr(d.getFileH))
	mux.HandleFunc("GET /api/v1/commits/{id}/files", hErr(d.listFilesH))
	mux.HandleFunc("DELETE /api/v1/commits/{id}", hErr(d.deleteCommitH))
	mux.HandleFunc("POST /api/v1/pipelines", hErr(d.createPipelineH))
	mux.HandleFunc("GET /api/v1/pipelines", hErr(d.listPipelinesH))
	mux.HandleFunc("GET /api/v1/pipelines/{name}", hErr(d.inspectPipelineH))
	mux.HandleFunc("DELETE /api/v1/pipelines/{name}", hErr(d.deletePipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/stop", hErr(d.stopPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/start", hErr(d.startPipelineH))
	mux.HandleFunc("POST /api/v1/pipelines/{name}/run", hErr(d.runPipelineH))
	mux.HandleFunc("GET /api/v1/jobs", hErr(d.listJobsH))
	mux.HandleFunc("GET /api/v1/jobs/{id}", hErr(d.inspectJobH))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums", hErr(d.listDatumsH))
	mux.HandleFunc("GET /api/v1/jobs/{id}/datums/{datumID}", hErr(d.inspectDatumH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/datums/{datumID}/restart", hErr(d.restartDatumH))
	mux.HandleFunc("GET /api/v1/logs", hErr(d.logsH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", hErr(d.cancelJobH))
	mux.HandleFunc("POST /api/v1/jobs/{id}/stop", hErr(d.cancelJobH))
	mux.HandleFunc("DELETE /api/v1/jobs/{id}", hErr(d.deleteJobH))
	mux.HandleFunc("POST /api/v1/transactions", hErr(d.startTransactionH))
	mux.HandleFunc("POST /api/v1/transactions/{id}/finish", hErr(d.finishTransactionH))
	mux.HandleFunc("DELETE /api/v1/transactions/{id}", hErr(d.deleteTransactionH))
	mux.HandleFunc("POST /api/v1/datums", hErr(d.enumerateDatumsH))
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
	w.Write(data)
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
			out = append(out, client.FileInfo{Path: path, Size: f.Size, Hash: f.SHA})
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
	if err := d.store.copyFile(r.PathValue("id"), r.PathValue("path"), body.SrcCommit, body.SrcPath); err != nil {
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
