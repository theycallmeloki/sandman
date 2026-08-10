// Package client is the black-box interface to Sandman's HTTP API.
//
// It is the executable contract of the behaviour specification: the
// conformance tests drive the system through this client, and the sandman
// CLI will wrap the same surface. Nothing here assumes an implementation.
//
// Execution-environment conventions (stated as contracts, used by tests):
//
//   - For each pipeline input, the job receives an environment variable named
//     after the input (its name, defaulting to the repository name) whose
//     value is the absolute path to that input's data directory (SB-096).
//     The name must be a valid shell identifier ([A-Za-z_][A-Za-z0-9_]*),
//     because it is used verbatim as an environment variable name.
//   - The job receives <NAME>_COMMIT = id of the input revision, OUT = path
//     to the output directory, JOB_ID = job id, OUTPUT_COMMIT = output
//     commit id (SB-051). These four names are reserved: a custom
//     environment variable may not shadow them. The output directory name
//     "out" is reserved and cannot be used as an input name (SB-170).
//   - A pipeline with no command and no stdin runs a default entry point
//     that copies every input file to the output unchanged (SB-126). A
//     pipeline with no command but with stdin is accepted at creation and
//     transitions to the failure state (SB-149).
//   - An input's glob is required at creation (SB-159, SB-170) and selects
//     which files of the input revision the job sees.
//   - A commit finished with the empty flag is explicitly empty: parent
//     content is not readable through it, even at the branch head (SB-118).
//   - A pipeline created after its input history exists processes the
//     branch head once, in one output commit (SB-023, SB-053); it does not
//     replay older history. A stopped pipeline ignores new commits; on
//     start it processes the backlog of commits finished while stopped
//     (SB-048).
//   - Writing a file into an open commit at a path already written in that
//     same commit is rejected (SB-156). Copying into a path that already
//     exists in the commit's view (including ancestors) is rejected. Jobs
//     are exempt: they upload into fresh output commits.
//   - Job inspection accepts a job id or an output commit id (SB-135).
//     Job listing filters by pipeline, by produced output commit, and by an
//     inclusive set of states; a full listing additionally carries each
//     job's transform and input spec (SB-093, SB-094, SB-095).
//   - Tags are durable global names bound to file content: put, get, and
//     list round-trip bytes exactly (SB-150).
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Error is a non-2xx API response carrying the server's message.
type Error struct {
	Status  int
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s (%d)", e.Message, e.Status)
}

// Client speaks Sandman's HTTP API.
type Client struct {
	base string
	hc   *http.Client
}

func New(addr string) *Client {
	return &Client{base: "http://" + addr, hc: &http.Client{Timeout: 60 * time.Second}}
}

// Base returns the server URL (http://host:port).
func (c *Client) Base() string { return c.base }

func (c *Client) do(method, p string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.base+p, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return &Error{Status: resp.StatusCode, Message: e.Error}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// doRaw is do with a raw byte body and raw byte response.
func (c *Client) doRaw(method, p string, body []byte) ([]byte, error) {
	req, err := http.NewRequest(method, c.base+p, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		json.Unmarshal(b, &e)
		return nil, &Error{Status: resp.StatusCode, Message: e.Error}
	}
	return b, nil
}

// doRawHeaders is doRaw that also returns the response headers.
func (c *Client) doRawHeaders(method, p string) (FileFetch, error) {
	req, err := http.NewRequest(method, c.base+p, nil)
	if err != nil {
		return FileFetch{}, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return FileFetch{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return FileFetch{}, err
	}
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		json.Unmarshal(b, &e)
		return FileFetch{}, &Error{Status: resp.StatusCode, Message: e.Error}
	}
	return FileFetch{
		Data:            b,
		ContentType:     resp.Header.Get("Content-Type"),
		ContentDisp:     resp.Header.Get("Content-Disposition"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
	}, nil
}

// ---- Repositories ----

type Repo struct {
	Name      string   `json:"name"`
	SizeBytes uint64   `json:"sizeBytes"` // total size of files at the head of the primary branch
	Branches  []string `json:"branches,omitempty"`
}

func (c *Client) CreateRepo(name string) error {
	return c.do("POST", "/api/v1/repos", map[string]string{"name": name}, nil)
}

// DeleteRepo removes a repository. force bypasses the guard that protects
// a pipeline's output repository from accidental deletion (SB-146); the
// internal spec repository is protected unconditionally (SB-127).
func (c *Client) DeleteRepo(name string, force bool) error {
	q := ""
	if force {
		q = "?force=1"
	}
	return c.do("DELETE", "/api/v1/repos/"+url.PathEscape(name)+q, nil, nil)
}

func (c *Client) ListRepos() ([]Repo, error) {
	var out []Repo
	return out, c.do("GET", "/api/v1/repos", nil, &out)
}

func (c *Client) InspectRepo(name string) (Repo, error) {
	var out Repo
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(name), nil, &out)
}

// ---- Commits ----

type Commit struct {
	ID          string `json:"id"`
	Repo        string `json:"repo"`
	Branch      string `json:"branch,omitempty"`
	Description string `json:"description,omitempty"`
	Started     bool   `json:"started"`
	Finished    bool   `json:"finished"`
	Empty       bool   `json:"empty,omitempty"`
	ParentID    string `json:"parentId,omitempty"`
}

func (c *Client) StartCommit(repo, branch, description string) (Commit, error) {
	var out Commit
	err := c.do("POST", "/api/v1/repos/"+url.PathEscape(repo)+"/commits",
		map[string]string{"branch": branch, "description": description}, &out)
	return out, err
}

func (c *Client) FinishCommit(id, description string, empty bool) (Commit, error) {
	var out Commit
	err := c.do("POST", "/api/v1/commits/"+url.PathEscape(id)+"/finish",
		map[string]any{"description": description, "empty": empty}, &out)
	return out, err
}

func (c *Client) InspectCommit(id string) (Commit, error) {
	var out Commit
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(id), nil, &out)
}

func (c *Client) HeadCommit(repo, branch string) (Commit, error) {
	var out Commit
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch)+"/head", nil, &out)
}

// CommitHistory walks the branch's commit chain oldest-first.
func (c *Client) CommitHistory(repo, branch string) ([]Commit, error) {
	head, err := c.HeadCommit(repo, branch)
	if err != nil {
		return nil, err
	}
	var chain []Commit
	for cur := &head; cur != nil && cur.ID != ""; {
		chain = append(chain, *cur)
		if cur.ParentID == "" {
			break
		}
		next, err := c.InspectCommit(cur.ParentID)
		if err != nil {
			return nil, err
		}
		cur = &next
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// ---- Files ----

type FileInfo struct {
	Path string `json:"path"`
	Size uint64 `json:"size"`
	// Hash is the hex sha256 of the file content (the datum identifier for
	// log filters, SB-060).
	Hash string `json:"hash,omitempty"`
}

func (c *Client) PutFile(commitID, p string, data []byte) error {
	_, err := c.doRaw("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), data)
	return err
}

func (c *Client) GetFile(commitID, p string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), nil)
}

func (c *Client) ListFiles(commitID string) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files", nil, &out)
}

// ---- Pipelines ----

// Transform is the execution description of a pipeline. Image is the
// container image (empty means "alpine"); when Cmd is empty the pipeline
// runs the default entry point (see the package comment). AcceptReturnCode
// declares one non-zero exit code that is treated as success (SB-033).
type Transform struct {
	Image            string            `json:"image,omitempty"`
	Cmd              []string          `json:"cmd,omitempty"`
	Stdin            []string          `json:"stdin,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	User             string            `json:"user,omitempty"`
	Workdir          string            `json:"workdir,omitempty"`
	AcceptReturnCode int               `json:"acceptReturnCode,omitempty"`
}

// Input is a file-scoped (PFS) input: files of the repo matched by Glob.
// Name aliases the input's environment variable and defaults to the repo
// name; it must be a valid shell identifier and not "out" (SB-170).
type Input struct {
	Name string `json:"name,omitempty"`
	Repo string `json:"repo"`
	Glob string `json:"glob,omitempty"`
}

type Parallelism struct {
	Constant    int     `json:"constant,omitempty"`
	Coefficient float64 `json:"coefficient,omitempty"`
}

type Pipeline struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Transform   *Transform   `json:"transform"`
	Input       *Input       `json:"input"`
	Parallelism *Parallelism `json:"parallelism,omitempty"`
	// Standby pipelines idle in the standby state and activate only when
	// work arrives, returning to standby once it settles (SB-049/050).
	Standby bool `json:"standby,omitempty"`
	// Update applies this spec to an existing pipeline (creating it when
	// absent, SB-040); Reprocess marks the update as a full reprocessing
	// request. Both are request flags, not persisted spec fields.
	Update    bool `json:"update,omitempty"`
	Reprocess bool `json:"reprocess,omitempty"`
}

// Pipeline state machine (P7): running, stopped, standby, failure, degraded,
// crashed. Stopped is a persistent flag distinct from the transient state:
// stopping a pipeline sets Stopped and reports state "paused" (SB-028).
// Transform and Input are populated in full inspections; the other fields
// are the current version's state.
type PipelineInfo struct {
	Name        string         `json:"name"`
	State       string         `json:"state"`
	Reason      string         `json:"reason,omitempty"`
	Description string         `json:"description,omitempty"`
	Stopped     bool           `json:"stopped"`
	Version     int            `json:"version,omitempty"`
	Transform   *Transform     `json:"transform,omitempty"`
	Input       *Input         `json:"input,omitempty"`
	JobCounts   map[string]int `json:"jobCounts,omitempty"` // jobs per terminal state (SB-029)
}

func (c *Client) CreatePipeline(p Pipeline) error {
	return c.do("POST", "/api/v1/pipelines", p, nil)
}

func (c *Client) ListPipelines() ([]PipelineInfo, error) {
	return c.ListPipelinesFiltered(nil, "", false)
}

// ListPipelinesFiltered lists pipelines. history < 0 lists every historical
// version of every pipeline; history >= 1 (or nil) lists only the most
// recent version of each pipeline; name restricts the listing to one
// pipeline (all its versions when history < 0). allowIncomplete includes
// pipelines whose definition is lost, as name-only entries (SB-143, SB-144).
func (c *Client) ListPipelinesFiltered(history *int, name string, allowIncomplete bool) ([]PipelineInfo, error) {
	q := url.Values{}
	if history != nil {
		q.Set("history", fmt.Sprintf("%d", *history))
	}
	if name != "" {
		q.Set("name", name)
	}
	if allowIncomplete {
		q.Set("allowIncomplete", "1")
	}
	qs := ""
	if len(q) > 0 {
		qs = "?" + q.Encode()
	}
	var out []PipelineInfo
	return out, c.do("GET", "/api/v1/pipelines"+qs, nil, &out)
}

func (c *Client) InspectPipeline(name string) (PipelineInfo, error) {
	return c.InspectPipelineVersion(name, 0)
}

// InspectPipelineVersion inspects the pipeline at ancestry depth k: 0 is
// the current version, 1 the previous, up to the oldest (SB-136).
func (c *Client) InspectPipelineVersion(name string, ancestry int) (PipelineInfo, error) {
	var out PipelineInfo
	q := ""
	if ancestry > 0 {
		q = fmt.Sprintf("?ancestry=%d", ancestry)
	}
	return out, c.do("GET", "/api/v1/pipelines/"+url.PathEscape(name)+q, nil, &out)
}

func (c *Client) DeletePipeline(name string, force, keepRepo bool) error {
	q := url.Values{}
	if force {
		q.Set("force", "1")
	}
	if keepRepo {
		q.Set("keepRepo", "1")
	}
	qs := ""
	if len(q) > 0 {
		qs = "?" + q.Encode()
	}
	return c.do("DELETE", "/api/v1/pipelines/"+url.PathEscape(name)+qs, nil, nil)
}

func (c *Client) StopPipeline(name string) error {
	return c.do("POST", "/api/v1/pipelines/"+url.PathEscape(name)+"/stop", nil, nil)
}

func (c *Client) StartPipeline(name string) error {
	return c.do("POST", "/api/v1/pipelines/"+url.PathEscape(name)+"/start", nil, nil)
}

// ---- Jobs and flush ----

// Job states (P7): running, success, failure, killed, skipped.
type Job struct {
	ID           string   `json:"id"`
	Pipeline     string   `json:"pipeline"`
	State        string   `json:"state"`
	Reason       string   `json:"reason,omitempty"`
	InputCommits []string `json:"inputCommits,omitempty"`
	OutputCommit string   `json:"outputCommit,omitempty"`
	Started      string   `json:"started,omitempty"`
	Finished     string   `json:"finished,omitempty"`
	// Version is the pipeline version the job ran under; Transform and
	// Input are snapshots of that version's spec, populated only in full
	// listings (SB-094). Historical jobs keep their own version's spec
	// even after a pipeline update (SB-040).
	Version   int        `json:"version,omitempty"`
	Transform *Transform `json:"transform,omitempty"`
	Input     *Input     `json:"input,omitempty"`
}

func (c *Client) ListJobs() ([]Job, error) {
	return c.ListJobsFiltered(JobFilter{})
}

// JobFilter selects the jobs a listing returns. OutputCommit matches the
// commit a job produced (SB-093); States is an inclusive set (SB-095);
// Full includes each job's transform and input spec (SB-094); History is
// the version depth: 0 = current version only, N = current version plus N
// older versions, -1 = every version (SB-143).
type JobFilter struct {
	Pipeline     string
	OutputCommit string
	States       []string
	Full         bool
	History      *int
}

func (c *Client) ListJobsFiltered(f JobFilter) ([]Job, error) {
	q := url.Values{}
	if f.Pipeline != "" {
		q.Set("pipeline", f.Pipeline)
	}
	if f.OutputCommit != "" {
		q.Set("outputCommit", f.OutputCommit)
	}
	for _, s := range f.States {
		q.Add("state", s)
	}
	if f.Full {
		q.Set("full", "1")
	}
	if f.History != nil {
		q.Set("history", fmt.Sprintf("%d", *f.History))
	}
	var out []Job
	qs := ""
	if len(q) > 0 {
		qs = "?" + q.Encode()
	}
	return out, c.do("GET", "/api/v1/jobs"+qs, nil, &out)
}

func (c *Client) InspectJob(id string) (Job, error) {
	var out Job
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(id), nil, &out)
}

func (c *Client) CancelJob(id string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, nil)
}

// StopJob stops a running job: the in-flight work is interrupted and the
// job is recorded killed; later jobs are unaffected (SB-058).
func (c *Client) StopJob(id string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(id)+"/stop", nil, nil)
}

func (c *Client) DeleteJob(id string) error {
	return c.do("DELETE", "/api/v1/jobs/"+url.PathEscape(id), nil, nil)
}

// Reset removes every repository, pipeline, and job (SB-037). It is
// idempotent and requires healthy metadata (D-08).
func (c *Client) Reset() error {
	return c.do("POST", "/api/v1/reset", nil, nil)
}

// Flush waits until every job triggered by the commit — including jobs of
// downstream pipeline stages that consume its output commits — is terminal,
// and returns them, deduplicated per pipeline keeping the latest. A
// terminal snapshot is only final once a second read a moment later agrees:
// the job graph can still be growing (head backfill, downstream triggers),
// and returning on the first terminal poll would race that growth. When no
// job exists at all, the flush terminates empty once every pipeline that
// could schedule work for the commit's repository is terminal-for-
// scheduling (stopped, failed, or crashed) — otherwise it keeps waiting
// for the growth to appear. A timeout returns the jobs seen so far with
// their states as the error.
func (c *Client) Flush(commitID string, timeout time.Duration) ([]Job, error) {
	deadline := time.Now().Add(timeout)
	var commitRepo string
	for {
		jobs, err := c.ListJobs()
		if err != nil {
			return jobs, err
		}
		relevant := latestPerPipeline(downstreamJobs(jobs, commitID))
		if allTerminal(relevant) {
			time.Sleep(250 * time.Millisecond)
			jobs2, err := c.ListJobs()
			if err != nil {
				return jobs, err
			}
			relevant2 := latestPerPipeline(downstreamJobs(jobs2, commitID))
			if sameJobSet(relevant, relevant2) && allTerminal(relevant2) {
				return relevant2, nil
			}
			continue
		}
		if len(relevant) == 0 {
			if commitRepo == "" {
				if cm, err := c.InspectCommit(commitID); err == nil {
					commitRepo = cm.Repo
				}
			}
			if commitRepo != "" && c.consumersSettled(commitRepo) {
				time.Sleep(250 * time.Millisecond)
				jobs2, err := c.ListJobs()
				if err != nil {
					return jobs, err
				}
				relevant2 := latestPerPipeline(downstreamJobs(jobs2, commitID))
				if len(relevant2) == 0 && c.consumersSettled(commitRepo) {
					return nil, nil
				}
				continue
			}
		}
		if time.Now().After(deadline) {
			return relevant, fmt.Errorf("flush timeout: %d job(s) not terminal", len(relevant))
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// consumersSettled reports whether every pipeline consuming the repo is
// terminal-for-scheduling: only a pipeline whose state is "running" can
// still create a job for a finished commit (backfill, update reprocessing).
func (c *Client) consumersSettled(repo string) bool {
	pipes, err := c.ListPipelines()
	if err != nil {
		return false
	}
	consumers := 0
	for _, p := range pipes {
		if p.Input != nil && p.Input.Repo == repo {
			consumers++
			if p.State == "running" {
				return false
			}
		}
	}
	return consumers > 0
}

func allTerminal(jobs []Job) bool {
	if len(jobs) == 0 {
		return false
	}
	for _, j := range jobs {
		if j.State == "running" {
			return false
		}
	}
	return true
}

func sameJobSet(a, b []Job) bool {
	if len(a) != len(b) {
		return false
	}
	ids := map[string]bool{}
	for _, j := range a {
		ids[j.ID] = true
	}
	for _, j := range b {
		if !ids[j.ID] {
			return false
		}
	}
	return true
}

// downstreamJobs walks the job graph from an input commit: every job whose
// input commits include it, then every job consuming those jobs' output
// commits, transitively.
func downstreamJobs(jobs []Job, commitID string) []Job {
	seen := map[string]bool{}
	var out []Job
	queue := []string{commitID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		for _, j := range jobs {
			if seen[j.ID] || j.OutputCommit == "" {
				continue
			}
			for _, ic := range j.InputCommits {
				if ic == id {
					seen[j.ID] = true
					out = append(out, j)
					queue = append(queue, j.OutputCommit)
					break
				}
			}
		}
	}
	return out
}

// latestPerPipeline keeps only the newest job of each pipeline (Started is
// RFC3339, so byte order is time order) — e.g. after a pipeline was deleted
// and recreated, flushing a shared input commit reports the fresh
// incarnation's job (SB-024).
func latestPerPipeline(jobs []Job) []Job {
	best := map[string]Job{}
	var order []string
	for _, j := range jobs {
		if prev, ok := best[j.Pipeline]; !ok || j.Started > prev.Started {
			if !ok {
				order = append(order, j.Pipeline)
			}
			best[j.Pipeline] = j
		}
	}
	out := make([]Job, 0, len(order))
	for _, p := range order {
		out = append(out, best[p])
	}
	return out
}

// ---- Tags (SB-150) ----

type TagInfo struct {
	Name string `json:"name"`
	Ref  string `json:"ref"` // non-empty reference to the tagged object
}

func (c *Client) PutTag(name string, data []byte) error {
	_, err := c.doRaw("PUT", "/api/v1/tags/"+url.PathEscape(name), data)
	return err
}

func (c *Client) GetTag(name string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/tags/"+url.PathEscape(name), nil)
}

func (c *Client) ListTags() ([]TagInfo, error) {
	var out []TagInfo
	return out, c.do("GET", "/api/v1/tags", nil, &out)
}

// ---- File copy and fetch ----

// CopyFile copies a file (or a directory subtree) from one commit into an
// open commit at dstPath. The destination must not already exist in the
// destination commit's view (SB-156).
func (c *Client) CopyFile(dstCommit, dstPath, srcCommit, srcPath string) error {
	return c.do("POST", "/api/v1/commits/"+url.PathEscape(dstCommit)+"/files/"+url.PathEscape(dstPath),
		map[string]string{"srcCommit": srcCommit, "srcPath": srcPath}, nil)
}

// FileFetch is the response of a raw file GET (SB-099).
type FileFetch struct {
	Data            []byte
	ContentType     string
	ContentDisp     string // "" unless download=true
	ContentEncoding string
}

// FetchFile GETs a file's raw bytes with response headers. With download
// true the server attaches an attachment Content-Disposition.
func (c *Client) FetchFile(commitID, p string, download bool) (FileFetch, error) {
	u := "/api/v1/commits/" + url.PathEscape(commitID) + "/files/" + url.PathEscape(p)
	if download {
		u += "?download=true"
	}
	data, err := c.doRawHeaders("GET", u)
	return data, err
}

// ---- Transactions (SB-162/163) ----

// StartTransaction opens a transaction and returns its id.
func (c *Client) StartTransaction() (string, error) {
	var out struct {
		ID string `json:"id"`
	}
	if err := c.do("POST", "/api/v1/transactions", nil, &out); err != nil {
		return "", err
	}
	return out.ID, nil
}

// CreatePipelineTx stages a pipeline create (or, with p.Update, an update)
// into an open transaction; it takes effect only when the transaction
// finishes. Pipelines staged together may consume each other's output
// (SB-162).
func (c *Client) CreatePipelineTx(p Pipeline, tx string) error {
	return c.do("POST", "/api/v1/pipelines?transaction="+url.QueryEscape(tx), p, nil)
}

// FinishTransaction applies every staged operation atomically: either all
// take effect or none do. It fails with an "outside of transaction" error
// when a staged pipeline was modified outside the transaction meanwhile
// (SB-163).
func (c *Client) FinishTransaction(tx string) error {
	return c.do("POST", "/api/v1/transactions/"+url.PathEscape(tx)+"/finish", nil, nil)
}

// DeleteTransaction aborts an open transaction, discarding its staged ops.
func (c *Client) DeleteTransaction(tx string) error {
	return c.do("DELETE", "/api/v1/transactions/"+url.PathEscape(tx), nil, nil)
}

// ---- Client-side detailed rendering (SB-036) ----

func (c *Client) DescribeRepo(name string) (string, error) {
	r, err := c.InspectRepo(name)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Repository: %s\nSizeBytes: %d\nBranches: %v\n", r.Name, r.SizeBytes, r.Branches), nil
}

func (c *Client) DescribeCommit(id string) (string, error) {
	cm, err := c.InspectCommit(id)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Commit: %s\nRepo: %s\nBranch: %s\nDescription: %s\nStarted: %v Finished: %v Empty: %v Parent: %s\n",
		cm.ID, cm.Repo, cm.Branch, cm.Description, cm.Started, cm.Finished, cm.Empty, cm.ParentID), nil
}

func (c *Client) DescribeFile(commitID, p string) (string, error) {
	data, err := c.GetFile(commitID, p)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("File: %s\nSizeBytes: %d\n", p, len(data)), nil
}

func (c *Client) DescribePipeline(name string) (string, error) {
	p, err := c.InspectPipeline(name)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Pipeline: %s\nState: %s\nStopped: %v\nDescription: %s\nVersion: %d\n",
		p.Name, p.State, p.Stopped, p.Description, p.Version), nil
}

func (c *Client) DescribeJob(id string) (string, error) {
	j, err := c.InspectJob(id)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Job: %s\nPipeline: %s\nState: %s\nOutputCommit: %s\nInputCommits: %v\n",
		j.ID, j.Pipeline, j.State, j.OutputCommit, j.InputCommits), nil
}

// ---- Logs (SB-059/060/061, D-21) ----

// LogParams selects the log lines a query returns. A datum filter narrows
// by an input file of the jobs (matched by path or by content hash, which
// is hex sha256 as returned by file listings) and requires a pipeline or
// job filter. Since is a relative window: lines older than now−Since are
// excluded. An empty filter set queries every job's logs (D-21: logs from
// all jobs are searchable globally).
type LogParams struct {
	Pipeline  string
	Job       string
	DatumPath string // input file path filter
	Datum     string // input file path or content hash filter
	Since     time.Duration
}

func (p LogParams) query(follow bool) string {
	q := url.Values{}
	if p.Pipeline != "" {
		q.Set("pipeline", p.Pipeline)
	}
	if p.Job != "" {
		q.Set("job", p.Job)
	}
	if p.DatumPath != "" {
		q.Set("datumPath", p.DatumPath)
	}
	if p.Datum != "" {
		q.Set("datum", p.Datum)
	}
	if p.Since != 0 {
		q.Set("since", p.Since.String())
	}
	if follow {
		q.Set("follow", "1")
	}
	return q.Encode()
}

// Logs returns the matching log lines, oldest first.
func (c *Client) Logs(p LogParams) ([]string, error) {
	var out struct {
		Lines []string `json:"lines"`
	}
	if err := c.do("GET", "/api/v1/logs?"+p.query(false), nil, &out); err != nil {
		return nil, err
	}
	return out.Lines, nil
}

// FollowLogs opens a live log stream: newline-delimited JSON objects
// {"line": "..."} for lines captured after the request began (live only).
// The returned body must be closed to end the stream. The stream uses a
// dedicated client with no timeout; follow mode never terminates on its
// own.
func (c *Client) FollowLogs(p LogParams) (io.ReadCloser, error) {
	req, err := http.NewRequest("GET", c.base+"/api/v1/logs?"+p.query(true), nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		var e struct {
			Error string `json:"error"`
		}
		json.NewDecoder(resp.Body).Decode(&e)
		return nil, &Error{Status: resp.StatusCode, Message: e.Error}
	}
	return resp.Body, nil
}
