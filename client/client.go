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
	base  string
	hc    *http.Client
	token string
}

func New(addr string) *Client {
	return &Client{base: "http://" + addr, hc: &http.Client{Timeout: 60 * time.Second}}
}

// SetToken configures the credential sent with every request (SB-154).
func (c *Client) SetToken(token string) { c.token = token }

// Base returns the server URL (http://host:port).
func (c *Client) Base() string { return c.base }

func (c *Client) do(method, p string, in, out any) error {
	return c.doClient(c.hc, method, p, in, out)
}

// doClient is do with a specific HTTP client (a long-poll needs a client
// timeout that outlives the server's own deadline, D-23 R-5).
func (c *Client) doClient(hc *http.Client, method, p string, in, out any) error {
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
	if c.token != "" {
		req.Header.Set("X-Sandbox-Token", c.token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&e)
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
	if c.token != "" {
		req.Header.Set("X-Sandbox-Token", c.token)
	}
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
		_ = json.Unmarshal(b, &e)
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
		_ = json.Unmarshal(b, &e)
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
	// CreatedAt is the commit's creation time (UTC RFC3339Nano).
	CreatedAt string `json:"createdAt,omitempty"`
	Finished  bool   `json:"finished"`
	Empty     bool   `json:"empty,omitempty"`
	ParentID  string `json:"parentId,omitempty"`
	// Provenance is the revision's derivation: the source commits it
	// consumes, transitively (a spout commit records its pipeline's
	// specification commit — its provenance epoch; a job's output records
	// its inputs and their provenance). Subvenants are the commits that
	// derive from this one, recorded only by InspectCommit.
	Provenance []string `json:"provenance,omitempty"`
	Subvenants []string `json:"subvenants,omitempty"`
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

// DeleteCommit removes a commit and everything derived from it across the
// DAG (SB-124): the commit's record, every downstream commit whose
// provenance includes it, and the jobs that consumed them; surviving
// commits' parent links are repaired and branch heads that pointed at a
// removed commit move to the nearest surviving ancestor or disappear. Ref
// is a commit id or repo@branch (the branch head). Deleting a branch head
// supersedes an in-flight job processing it (SB-125).
func (c *Client) DeleteCommit(ref string) error {
	return c.do("DELETE", "/api/v1/commits/"+url.PathEscape(ref), nil, nil)
}

func (c *Client) HeadCommit(repo, branch string) (Commit, error) {
	var out Commit
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch)+"/head", nil, &out)
}

// CreateBranch points a branch at an existing commit — creating the branch
// or retargeting it (SB-142). Pipelines watching the branch process the
// commit exactly once.
func (c *Client) CreateBranch(repo, branch, head string) error {
	return c.do("POST", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch), map[string]string{"head": head}, nil)
}

// Branch is a named pointer into a repo's commit history (SB-142): Head is
// the id of the commit the branch currently points at.
type Branch struct {
	Repo   string `json:"repo"`
	Branch string `json:"branch"`
	Head   string `json:"head"`
}

// ListBranches returns every branch of the repo with its head commit id.
func (c *Client) ListBranches(repo string) ([]Branch, error) {
	var out []Branch
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(repo)+"/branches", nil, &out)
}

// InspectBranch returns the named branch's head commit id.
func (c *Client) InspectBranch(repo, branch string) (Branch, error) {
	var out Branch
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch), nil, &out)
}

// DeleteBranch removes the branch ref (the repo's default branch is
// protected). The branch's commits stay addressable by id.
func (c *Client) DeleteBranch(repo, branch string) error {
	return c.do("DELETE", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch), nil, nil)
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

// PutFileOverwrite writes a file replacing any content accumulated at the
// path (FS-3): the prior content — in this commit or its ancestry — is
// removed and the new bytes become the entire content at this revision.
func (c *Client) PutFileOverwrite(commitID, p string, data []byte) error {
	_, err := c.doRaw("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+"?overwrite=1", data)
	return err
}

// PutFileSplit uploads data split into records at delimiter (SB-137/138):
// each record is stored at p/<index>. With header, the first chunk is the
// header and is replicated into every record's file — appending records
// under the same header leaves earlier records' identity unchanged, so
// the dedup skips them; a changed header re-identifies every record.
func (c *Client) PutFileSplit(commitID, p string, data []byte, delimiter string, header bool) error {
	q := "?split=1&delimiter=" + url.QueryEscape(delimiter)
	if header {
		q += "&header=1"
	}
	_, err := c.doRaw("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+q, data)
	return err
}

// PutFileURL fetches a file from an HTTP URL and stores it in the commit
// (SB-088).
func (c *Client) PutFileURL(commitID, p, u string) error {
	return c.do("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+"?fetch="+url.QueryEscape(u), nil, nil)
}

// DeleteFile tombstones a path in an open commit: the path is removed from
// the branch's view at this revision (SB-007).
func (c *Client) DeleteFile(commitID, p string) error {
	return c.do("DELETE", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), nil, nil)
}

func (c *Client) GetFile(commitID, p string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), nil)
}

// ServiceInfo is a service pipeline's running record (SB-100 clause 4):
// the declared ports and the endpoint's annotations — the user's own plus
// the system's identifying pipelineName annotation.
type ServiceInfo struct {
	Pipeline    string            `json:"pipeline"`
	Internal    int               `json:"internalPort"`
	External    int               `json:"externalPort"`
	Annotations map[string]string `json:"annotations"`
}

// InspectService returns a service pipeline's record (SB-100).
func (c *Client) InspectService(pipeline string) (ServiceInfo, error) {
	var out ServiceInfo
	return out, c.do("GET", "/api/v1/services/"+url.PathEscape(pipeline), nil, &out)
}

// ListServices lists every service pipeline's record.
func (c *Client) ListServices() ([]ServiceInfo, error) {
	var out []ServiceInfo
	return out, c.do("GET", "/api/v1/services", nil, &out)
}

// ServiceProxy fetches a path through the control plane's per-pipeline
// service proxy (SB-100 clause 3): the request is forwarded to the
// service's own endpoint, so the response is identical to the direct one.
func (c *Client) ServiceProxy(pipeline, p string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/services/"+url.PathEscape(pipeline)+"/"+url.PathEscape(p), nil)
}

func (c *Client) ListFiles(commitID string) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files", nil, &out)
}

// ListFilesGlob lists a commit's files whose paths start with the given
// prefix ("1*" lists the paths beginning with "1"; SB-047 clause 4). The
// server applies the filter; only prefix patterns are supported.
func (c *Client) ListFilesGlob(commitID, glob string) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files?glob="+url.QueryEscape(glob), nil, &out)
}

// ListFileHistory lists the revisions of a path across the commit's
// ancestry: one FileInfo per ancestor revision where the path resolves,
// newest first (SB-145). A negative limit returns every revision.
func (c *Client) ListFileHistory(commitID, p string, limit int) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+
		fmt.Sprintf("?history=1&limit=%d", limit), nil, &out)
}

// ---- Pipelines ----

// Transform is the execution description of a pipeline. Image is the
// container image (empty means "alpine"); when Cmd is empty the pipeline
// runs the default entry point (see the package comment). AcceptReturnCode
// declares one non-zero exit code that is treated as success (SB-033).
// DatumTries retries a failing datum that many times before it is marked
// failed (SB-134); a value of zero means one attempt. ErrCmd is a
// secondary command run when the primary fails for a datum: if it
// succeeds the datum is recovered, not failed (SB-012). DatumTimeout and
// JobTimeout bound a datum's and a whole job's execution (SB-113/115/116).
type Transform struct {
	Image            string            `json:"image,omitempty"`
	Cmd              []string          `json:"cmd,omitempty"`
	Stdin            []string          `json:"stdin,omitempty"`
	ErrCmd           []string          `json:"errCmd,omitempty"`
	ErrStdin         []string          `json:"errStdin,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	User             string            `json:"user,omitempty"`
	Workdir          string            `json:"workdir,omitempty"`
	AcceptReturnCode int               `json:"acceptReturnCode,omitempty"`
	DatumTries       int               `json:"datumTries,omitempty"`
	DatumTimeout     string            `json:"datumTimeout,omitempty"`
	JobTimeout       string            `json:"jobTimeout,omitempty"`
	// PodSpec is a full execution-environment customization document
	// (SB-072): a JSON object {"env": {...}, "volumes": {...},
	// "workdir": ...} applied to every execution participant. PodPatch is
	// a JSON modification list (RFC 6902 operations) applied to the
	// document after the spec. Both are validated as JSON at creation;
	// malformed customization fails pipeline creation (SB-072 clause 1,
	// SB-152).
	PodSpec  string `json:"podSpec,omitempty"`
	PodPatch string `json:"podPatch,omitempty"`
	// Secrets binds named secrets from the system's secret store into
	// every job's environment (SB-051 clause 2, D-05): a mount's Key is
	// written as a file at MountPath/<key> and/or injected as the EnvVar.
	// Each reference names an existing secret — the binding is explicit.
	Secrets []SecretMount `json:"secrets,omitempty"`
	// ResourceRequests and ResourceLimits declare the execution
	// environment's resources (SB-067/068/069/070): Memory as a size
	// string ("100M"), CPU as a fractional core count, Disk as a size
	// string. Sandbox deviation: docker expresses a CPU request only as
	// an allocation, and a disk request is recorded but not enforceable
	// on docker's default driver.
	ResourceRequests *ResourceRequests `json:"resourceRequests,omitempty"`
	ResourceLimits   *ResourceLimits   `json:"resourceLimits,omitempty"`
}

// ResourceRequests are the pipeline's requested resources (SB-067).
type ResourceRequests struct {
	Memory string  `json:"memory,omitempty"`
	CPU    float64 `json:"cpu,omitempty"`
	Disk   string  `json:"disk,omitempty"`
}

// ResourceLimits are the pipeline's hard resource limits (SB-068).
type ResourceLimits struct {
	Memory string  `json:"memory,omitempty"`
	CPU    float64 `json:"cpu,omitempty"`
}

// Input is a file-scoped (PFS) input: files of the repo matched by Glob.
// Name aliases the input's environment variable and defaults to the repo
// name; it must be a valid shell identifier and not "out" (SB-170).
// Branch defaults to "master". Cross, when non-empty, makes the input the
// cartesian product of the listed inputs (each member is itself a
// file-scoped input with its own repo, glob, branch, and name); a
// file-scoped Input with Cross set is invalid.
// Trigger is a size-based commit trigger (SB-160): bytes newly committed
// to the watched branch accumulate, and when the accumulated total
// reaches SizeBytes the pipeline runs on the accumulated data. The input
// reads its dedicated accumulation branch (derived from the pipeline and
// the input's position), not the watched branch.
type Trigger struct {
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// Branch is the watched branch (default "master").
	Branch string `json:"branch,omitempty"`
}

// GitInput declares an external git repository as a pipeline input
// (SB-104..112): the repository at URL is mapped to an auto-created
// repository named by the input's own Name (SB-109) or derived from the
// URL when the input has no name, and a push event for URL on the tracked
// branch commits the pushed revision into that repository and triggers
// the pipeline.
type GitInput struct {
	URL string `json:"url"`
	// Branch is the tracked branch (default "master"): pushes to any
	// other branch are ignored entirely (SB-112).
	Branch string `json:"branch,omitempty"`
}

type Input struct {
	Name   string  `json:"name,omitempty"`
	Repo   string  `json:"repo,omitempty"`
	Glob   string  `json:"glob,omitempty"`
	Branch string  `json:"branch,omitempty"`
	Cross  []Input `json:"cross,omitempty"`
	// Join, when non-empty, makes the input a join: each member's glob
	// captures a key (JoinOn selects the captured group, e.g. "$1" or
	// "$1$3") and a datum exists for every key present in all members,
	// containing one file per member (SB-074). Outer marks a member whose
	// unmatched keys still produce datums, each carrying only that
	// member's file — the unmatched members' directories are absent
	// (SB-075). A file-scoped Input with Join set is invalid.
	Join   []Input `json:"join,omitempty"`
	JoinOn string  `json:"joinOn,omitempty"`
	Outer  bool    `json:"outer,omitempty"`
	// Group, when non-empty, makes the input a group: files across all
	// members are collected by their GroupBy capture value into one datum
	// per key (union, not cross product) (SB-076). A member with JoinOn
	// joins first, then groups the joined pairs.
	Group   []Input `json:"group,omitempty"`
	GroupBy string  `json:"groupBy,omitempty"`
	// Union, when non-empty, makes the input a union: the members'
	// (branches') files are exposed under this input's namespace, and
	// files at the same path from different branches merge by
	// concatenation in branch order — one datum per distinct path
	// (SB-077/078). A member may be a plain repo, a cross, or a nested
	// union; the exposed namespace is the member's own Name.
	Union []Input `json:"union,omitempty"`
	// Cron makes the input a scheduled cron input (SB-089/133): the
	// schedule is an "@every <duration>" spec, and at each tick the
	// system commits a file named by the tick time (UTC RFC3339) to the
	// input's auto-created repository (named after the pipeline and the
	// input). Overwrite makes each tick's commit replace the previous
	// tick's file instead of accumulating. A cron input needs no repo or
	// glob.
	Cron      string `json:"cron,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
	// Git makes the input a git input (SB-104..112): the external
	// repository at URL is mapped to an auto-created repository named by
	// the input's own Name (SB-109) or derived from the URL when unnamed;
	// a push event for URL on the tracked branch (Branch, default
	// "master") commits the pushed revision into the mapped repository
	// and triggers the pipeline. A git input needs no repo or glob.
	Git *GitInput `json:"git,omitempty"`
	// Trigger makes the input a size trigger (SB-160): bytes newly
	// committed to the watched branch accumulate, and every completed
	// threshold unit runs the pipeline on the accumulated data, from the
	// input's dedicated accumulation branch.
	Trigger *Trigger `json:"trigger,omitempty"`
	// Lazy marks the input as materialized on demand rather than eagerly
	// (SB-014/015/017). The flag is part of the pipeline spec and is
	// recorded on every job's input snapshot; sandman materializes the
	// files for execution either way.
	Lazy bool `json:"lazy,omitempty"`
}

// InputBranch is the effective branch of an input side.
func InputBranch(s Input) string {
	if s.Branch == "" {
		return "master"
	}
	return s.Branch
}

// InputSides normalizes an input into its constituent sides: a file-scoped
// input is its own single side; a cross input is its members in
// declaration order.
func InputSides(in *Input) []Input {
	if in == nil {
		return nil
	}
	if len(in.Cross) > 0 {
		return in.Cross
	}
	if len(in.Join) > 0 {
		return in.Join
	}
	if len(in.Group) > 0 {
		return in.Group
	}
	if len(in.Union) > 0 {
		return in.Union
	}
	return []Input{*in}
}

type Parallelism struct {
	Constant    int     `json:"constant,omitempty"`
	Coefficient float64 `json:"coefficient,omitempty"`
}

// ChunkSpec configures how a side's glob matches are grouped into datums
// (SB-102): either a target number of datums or a target chunk size. It is
// an internal scheduling knob — the output must be identical regardless.
type ChunkSpec struct {
	Number    int `json:"number,omitempty"`
	SizeBytes int `json:"sizeBytes,omitempty"`
}

// WorkerStatus is one worker's live state while its job runs (SB-065/097):
// the datum it is processing, when it started, and how many datums are
// queued behind it (bounded by the pipeline's maxQueueSize).
type WorkerStatus struct {
	Worker  int    `json:"worker"`
	Datum   string `json:"datum,omitempty"`
	Started string `json:"started,omitempty"`
	Queue   int    `json:"queue"`
}

type Pipeline struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Transform   *Transform   `json:"transform"`
	Input       *Input       `json:"input"`
	Parallelism *Parallelism `json:"parallelism,omitempty"`
	ChunkSpec   *ChunkSpec   `json:"chunkSpec,omitempty"`
	// MaxQueueSize bounds each worker's queued (not yet started) datums;
	// a value of zero means a queue of one (SB-097). Autoscaling derives
	// the execution scale from the datum count, capped at the configured
	// parallelism (SB-165; the cap is never exceeded, and never more
	// workers than datums).
	MaxQueueSize int  `json:"maxQueueSize,omitempty"`
	Autoscaling  bool `json:"autoscaling,omitempty"`
	// Standby pipelines idle in the standby state and activate only when
	// work arrives, returning to standby once it settles (SB-049/050).
	Standby bool `json:"standby,omitempty"`
	// OutputBranch names the branch the pipeline writes its output to
	// (default "master"); a downstream pipeline that watches a different
	// branch of the output repo does not run until that branch is pointed
	// at the output commit (SB-142).
	OutputBranch string `json:"outputBranch,omitempty"`
	// Reprocess is a persisted spec field: every job re-executes all of
	// its datums instead of skipping datums unchanged from a previous
	// successful run (SB-166; D-13 — an update that sets it requests full
	// reprocessing). EnableStats persists per-datum statistics: a stats
	// branch on the output repo and inspectable datum records (SB-080);
	// it is one-way — an update cannot disable it (SB-081). Update is a
	// request flag (create when absent, SB-040).
	Update      bool `json:"update,omitempty"`
	Reprocess   bool `json:"reprocess,omitempty"`
	EnableStats bool `json:"enableStats,omitempty"`
	// Placement names the placement label a host must bear for this
	// pipeline's jobs to run: work is scheduled onto a registered
	// execution host bearing the label, and the pipeline never names a
	// specific host address (SB-167). Empty means unconstrained — the
	// control plane's own executor runs the jobs (SB-169: unplaceable
	// work surfaces as the crashed pipeline state until a host returns).
	Placement string `json:"placement,omitempty"`
	// Framework names an execution framework; sandman supports none, so
	// any non-empty value is rejected at creation naming the framework
	// (SB-151).
	Framework string `json:"framework,omitempty"`
	// Spout, when set, makes the pipeline a spout (SB-139): it must not
	// declare an input, its transform runs in the background, and the
	// daemon commits each data-bearing cycle to the output branch.
	Spout *Spout `json:"spout,omitempty"`
	// Service, when set, makes the pipeline a service (SB-100): instead
	// of settling after processing a revision, one long-running process
	// serves the pipeline's input over HTTP, and new input revisions are
	// served automatically without redeploying. InternalPort is where the
	// user's process listens inside the execution environment;
	// ExternalPort is bound on the control-plane host and proxied to the
	// process wherever it runs (SB-168). Annotations are preserved on the
	// service record alongside the system's own pipelineName annotation.
	Service *Service `json:"service,omitempty"`
	// Egress, when set, copies the job's finished output to an external
	// destination after the output commit succeeds; a failed egress write
	// fails the job with an egress-related reason even though the output
	// commit itself succeeded (SB-013).
	Egress *Egress `json:"egress,omitempty"`
}

// SecretMount references one secret's key into the execution
// environment (SB-051): MountPath mounts the key's value as a file at
// MountPath/<key> (paths must live under /sandman/ — the execution
// namespace); EnvVar injects the key's value as that environment
// variable. At least one of the two must be set.
type SecretMount struct {
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
	MountPath string `json:"mountPath,omitempty"`
	EnvVar    string `json:"envVar,omitempty"`
}

// Egress is a pipeline's external output destination (SB-013, D-17).
// URL names the destination; file:// destinations copy the output files
// into the target directory, and any other scheme is refused at egress
// time, failing the job.
type Egress struct {
	URL string `json:"url"`
}

// Service is a service pipeline's declaration (SB-100/168).
type Service struct {
	InternalPort int               `json:"internalPort"`
	ExternalPort int               `json:"externalPort"`
	Annotations  map[string]string `json:"annotations,omitempty"`
}

// Spout is a spout pipeline's options (SB-139): Overwrite replaces the
// committed file content each cycle instead of accumulating it, and
// Marker names a directory whose files are committed to a separate marker
// branch (a name with glob metacharacters is rejected).
type Spout struct {
	Overwrite bool   `json:"overwrite,omitempty"`
	Marker    string `json:"marker,omitempty"`
}

// Pipeline state machine (P7): running, stopped, standby, failure, crashed. Stopped is a persistent flag distinct from the transient state:
// stopping a pipeline sets Stopped and reports state "paused" (SB-028).
// Transform and Input are populated in full inspections; the other fields
// are the current version's state.
type PipelineInfo struct {
	Name         string         `json:"name"`
	State        string         `json:"state"`
	Reason       string         `json:"reason,omitempty"`
	Description  string         `json:"description,omitempty"`
	Stopped      bool           `json:"stopped"`
	Version      int            `json:"version,omitempty"`
	Transform    *Transform     `json:"transform,omitempty"`
	Input        *Input         `json:"input,omitempty"`
	Parallelism  *Parallelism   `json:"parallelism,omitempty"`
	ChunkSpec    *ChunkSpec     `json:"chunkSpec,omitempty"`
	MaxQueueSize int            `json:"maxQueueSize,omitempty"`
	Autoscaling  bool           `json:"autoscaling,omitempty"`
	Standby      bool           `json:"standby,omitempty"`
	OutputBranch string         `json:"outputBranch,omitempty"`
	Reprocess    bool           `json:"reprocess,omitempty"`
	EnableStats  bool           `json:"enableStats,omitempty"`
	Spout        *Spout         `json:"spout,omitempty"`
	Service      *Service       `json:"service,omitempty"`
	Egress       *Egress        `json:"egress,omitempty"`
	Secrets      []SecretMount  `json:"secrets,omitempty"`
	Placement    string         `json:"placement,omitempty"`
	JobCounts    map[string]int `json:"jobCounts,omitempty"` // jobs per terminal state (SB-029)
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

// RunPipeline manually triggers a pipeline run (SB-010). Provenance, when
// non-empty, is the exact set of input revisions the job must process —
// not the branch heads; when empty, the current branch heads are used.
// JobID, when non-empty, targets an existing job whose input pairing is
// re-executed. A run never propagates downstream.
func (c *Client) RunPipeline(name string, provenance []string, jobID string) (Job, error) {
	var out Job
	body := map[string]any{}
	if len(provenance) > 0 {
		body["provenance"] = provenance
	}
	if jobID != "" {
		body["job"] = jobID
	}
	return out, c.do("POST", "/api/v1/pipelines/"+url.PathEscape(name)+"/run", body, &out)
}

// TriggerCron creates an immediate tick on every cron input of the
// pipeline (SB-089): a commit lands now regardless of the schedule, and
// scheduled ticks keep flowing.
func (c *Client) TriggerCron(name string) error {
	return c.do("POST", "/api/v1/pipelines/"+url.PathEscape(name)+"/trigger", nil, nil)
}

// PushGitEvent delivers a git push event (SB-107..112): the daemon
// commits the pushed revision into every mapped repository bound to the
// URL (on the tracked branch) and triggers the consuming pipelines.
// files is the pushed revision's working tree (the external repository's
// content); private marks a repository the system cannot clone (no
// credentials) — the event is accepted but produces no commit and the
// bound pipelines fail with a reason naming the URL (SB-105).
func (c *Client) PushGitEvent(url, branch, revision string, files map[string]string, private bool) error {
	return c.do("POST", "/api/v1/git/push", map[string]any{
		"url":      url,
		"branch":   branch,
		"revision": revision,
		"files":    files,
		"private":  private,
	}, nil)
}

// ---- Datums ----

// Datum is one unit of a job's work: its identity and the input files it
// reads, one per input side (SB-080: a datum carries one input file per
// side). DatumFile names the input side and the file within it.
type Datum struct {
	ID    string      `json:"id"`
	Files []DatumFile `json:"files,omitempty"`
}

type DatumFile struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Hash string `json:"hash,omitempty"`
}

// EnumerateDatums lists the datum set an input would process at its
// current input revisions, without creating or running a pipeline (SB-161).
func (c *Client) EnumerateDatums(input Input) ([]Datum, error) {
	var out []Datum
	return out, c.do("POST", "/api/v1/datums", map[string]any{"input": input}, &out)
}

// DatumInfo is one datum's record for a job: identity, per-side input
// files, the produced output files, outcome state (running | success |
// recovered | failed | skipped), process time, and timing (SB-080/082/113).
type DatumInfo struct {
	ID          string      `json:"id"`
	State       string      `json:"state"`
	InputFiles  []DatumFile `json:"inputFiles,omitempty"`
	OutputFiles []DatumFile `json:"outputFiles,omitempty"`
	ProcessTime float64     `json:"processTime,omitempty"` // seconds
	Started     string      `json:"started,omitempty"`
	Finished    string      `json:"finished,omitempty"`
	Worker      int         `json:"worker,omitempty"`
	Reason      string      `json:"reason,omitempty"`
}

// DatumPage is a paginated datum listing (SB-080/083): the page's datums
// plus the total page count and the served (zero-based) page index.
type DatumPage struct {
	Datums     []DatumInfo `json:"datums"`
	TotalPages int         `json:"totalPages"`
	Page       int         `json:"page"`
}

// ListDatums lists a job's datums, state-ordered (failed first, skipped
// last — SB-082/084) and paginated. limit 0 requests everything; a page
// index at or beyond the page count errors (SB-083).
func (c *Client) ListDatums(jobID string, limit, page int) (DatumPage, error) {
	q := url.Values{}
	if limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", limit))
	}
	if page > 0 {
		q.Set("page", fmt.Sprintf("%d", page))
	}
	qs := ""
	if len(q) > 0 {
		qs = "?" + q.Encode()
	}
	var out DatumPage
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(jobID)+"/datums"+qs, nil, &out)
}

// InspectDatum returns one datum's record; it errors when the pipeline
// does not record per-datum statistics (SB-081).
func (c *Client) InspectDatum(jobID, datumID string) (DatumInfo, error) {
	var out DatumInfo
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(jobID)+"/datums/"+url.PathEscape(datumID), nil, &out)
}

// RestartDatum aborts a datum's current processing and starts it over:
// the next status observation shows it running with a later start time,
// and the job still completes (SB-064).
func (c *Client) RestartDatum(jobID, datumID string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(jobID)+"/datums/"+url.PathEscape(datumID)+"/restart", nil, nil)
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
	// StatsCommit is the job's per-datum statistics commit on the output
	// repo's "stats" branch, when statistics are enabled (SB-086/113).
	StatsCommit string `json:"statsCommit,omitempty"`
	// Per-datum outcome counts (SB-012): processed (success), recovered
	// (primary failed, error handler succeeded), failed, skipped (datum
	// unchanged from a previous successful run).
	Processed int `json:"processed,omitempty"`
	Recovered int `json:"recovered,omitempty"`
	Failed    int `json:"failed,omitempty"`
	Skipped   int `json:"skipped,omitempty"`
	// Workers is the live per-worker status while the job runs (SB-065).
	Workers []WorkerStatus `json:"workers,omitempty"`
}

func (c *Client) ListJobs() ([]Job, error) {
	return c.ListJobsFiltered(JobFilter{})
}

// JobFilter selects the jobs a listing returns. OutputCommit matches the
// commit a job produced (SB-093); States is an inclusive set (SB-095);
// Full includes each job's transform and input spec (SB-094); History is
// the version depth: 0 = current version only, N = current version plus N
// older versions, -1 = every version (SB-143). InputCommits returns only
// jobs whose input set includes every listed commit (SB-120).
type JobFilter struct {
	Pipeline     string
	OutputCommit string
	States       []string
	Full         bool
	History      *int
	InputCommits []string
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
	for _, ic := range f.InputCommits {
		q.Add("inputCommit", ic)
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
	// clause 1 (SB-135): inspecting with neither a job nor an output
	// commit identifier is rejected stating that one of the two must be
	// specified
	if id == "" {
		return out, fmt.Errorf("must specify a Job or an OutputCommit")
	}
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

// Version reports the control plane's version (the daemon's baked build
// version). `sandman --version` uses it to show both binary and daemon.
func (c *Client) Version() (string, error) {
	var out struct {
		Version string `json:"version"`
	}
	if err := c.do("GET", "/api/v1/version", nil, &out); err != nil {
		return "", err
	}
	return out.Version, nil
}

// FetchMetrics returns the runtime metrics in Prometheus exposition
// format: invocation counters and latency aggregates for file reads
// (split by outcome), file writes, and job listings (SB-132).
func (c *Client) FetchMetrics() (string, error) {
	b, err := c.doRaw("GET", "/api/v1/metrics", nil)
	return string(b), err
}

// CollectGarbage reclaims durable artifacts no longer referenced by any
// commit, tag, or spec record (SB-079). It fails while a job is running.
func (c *Client) CollectGarbage() error {
	return c.do("POST", "/api/v1/gc", nil, nil)
}

// Check runs the control plane's consistency check (SB-139 clause 14): an
// error reports corrupted metadata, nil reports an intact system.
func (c *Client) Check() error {
	return c.do("POST", "/api/v1/check", nil, nil)
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
	return c.flushSet([]string{commitID}, timeout)
}

// FlushSet flushes a set of input commits together: it returns only the
// jobs whose input set includes every commit in the set — a cross
// pipeline's pairing job — plus their downstream consumers, never the
// single-side jobs (SB-009, SB-120).
func (c *Client) FlushSet(commitIDs []string, timeout time.Duration) ([]Job, error) {
	return c.flushSet(commitIDs, timeout)
}

// flushSet blocks on the server-side flush (D-23 R-5): the daemon runs
// the flush loop — downstream closure terminal and stable — and the
// client waits on the long-poll response. A server timeout returns the
// jobs seen so far with a timeout error, preserving the flush contract.
func (c *Client) flushSet(commitIDs []string, timeout time.Duration) ([]Job, error) {
	var out struct {
		Jobs     []Job `json:"jobs"`
		TimedOut bool  `json:"timedOut"`
	}
	// the server returns at its own deadline; the HTTP client must
	// outlive it by a margin, or the request is cut mid-flush
	hc := &http.Client{Timeout: timeout + 30*time.Second}
	err := c.doClient(hc, "POST", "/api/v1/flush", map[string]any{
		"commits": commitIDs,
		"timeout": timeout.String(),
	}, &out)
	if err != nil {
		return out.Jobs, err
	}
	if out.TimedOut {
		return out.Jobs, fmt.Errorf("flush timeout: %d job(s) not terminal", len(out.Jobs))
	}
	return out.Jobs, nil
}

// WaitJob blocks until the job reaches a terminal state (D-23 R-5): the
// server long-polls its own state broadcasts, so the client makes one
// request. A timeout returns the job's current state with an error.
func (c *Client) WaitJob(jobID string, timeout time.Duration) (Job, error) {
	// the long-poll needs a client whose deadline outlives the server's
	// own wait deadline (D-23 R-5): equal deadlines race and the client
	// can time out "awaiting headers" just before the server's response
	hc := &http.Client{Timeout: timeout + 30*time.Second}
	var j Job
	err := c.doClient(hc, "GET", "/api/v1/jobs/"+url.PathEscape(jobID)+"/wait?timeout="+url.QueryEscape(timeout.String()), nil, &j)
	return j, err
}

// consumersSettled reports whether every pipeline that could schedule work
// for the commit on this branch is terminal. A consumer that watches a
// different branch of the repo cannot schedule work for the commit, so it
// is settled regardless of its state — a commit on a non-watched branch
// flushes to zero immediately (SB-142).
func AllTerminal(jobs []Job) bool {
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

func SameJobSet(a, b []Job) bool {
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

// DownstreamJobsSet returns every job in the graph whose input set
// includes every commit in the set (a cross pipeline's pairing), then
// every consumer of those jobs' outputs, transitively.
func DownstreamJobsSet(jobs []Job, commitIDs []string) []Job {
	if len(commitIDs) == 0 {
		return nil
	}
	// reference flush semantics: the set's downstream is the INTERSECTION
	// of each commit's downstream closure — a job is reported only if it
	// is downstream of every flushed commit (SB-056 wave 1: B's output is
	// downstream of A only, so it is excluded; C's pairing and D are
	// downstream of both). A single closure seeded by jobs whose inputs
	// contain every commit is wrong when the set spans a DAG: the pairing
	// job's inputs reference the upstream OUTPUT commit (B-out), not A's
	// raw commit, so no direct match exists to seed the walk.
	type closure struct {
		set map[string]bool
		ord []string
	}
	byID := map[string]Job{}
	for _, j := range jobs {
		byID[j.ID] = j
	}
	cl := make([]closure, len(commitIDs))
	for i, id := range commitIDs {
		c := closure{set: map[string]bool{}}
		seen := map[string]bool{}
		var queue []string
		for _, j := range jobs {
			for _, ic := range j.InputCommits {
				if ic == id {
					seen[j.ID] = true
					c.set[j.ID] = true
					c.ord = append(c.ord, j.ID)
					if j.OutputCommit != "" {
						queue = append(queue, j.OutputCommit)
					}
					if j.StatsCommit != "" {
						queue = append(queue, j.StatsCommit)
					}
					break
				}
			}
		}
		for len(queue) > 0 {
			head := queue[0]
			queue = queue[1:]
			for _, j := range jobs {
				if seen[j.ID] || j.OutputCommit == "" {
					continue
				}
				for _, ic := range j.InputCommits {
					if ic == head {
						seen[j.ID] = true
						c.set[j.ID] = true
						c.ord = append(c.ord, j.ID)
						queue = append(queue, j.OutputCommit)
						if j.StatsCommit != "" {
							queue = append(queue, j.StatsCommit)
						}
						break
					}
				}
			}
		}
		cl[i] = c
	}
	// intersect in first-closure order
	first := cl[0]
	var out []Job
	for _, id := range first.ord {
		ok := true
		for _, c := range cl[1:] {
			if !c.set[id] {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, byID[id])
		}
	}
	return out
}

// LatestPerPipeline keeps only the newest job of each pipeline (Started is
// RFC3339, so byte order is time order) — e.g. after a pipeline was deleted
// and recreated, flushing a shared input commit reports the fresh
// incarnation's job (SB-024).
func LatestPerPipeline(jobs []Job) []Job {
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

// ---- Secrets (SB-153/154) ----

// SecretInfo is a secret's inspectable record: name, type label, and the
// system-assigned creation timestamp (the data itself is write-only).
type SecretInfo struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Created string `json:"created"`
}

// CreateSecret stores a named typed metadata blob (SB-153). The type is
// reported as "Opaque" — sandman's only secret kind.
func (c *Client) CreateSecret(name string, data map[string]string) error {
	return c.do("POST", "/api/v1/secrets", map[string]any{"name": name, "data": data}, nil)
}

func (c *Client) InspectSecret(name string) (SecretInfo, error) {
	var out SecretInfo
	return out, c.do("GET", "/api/v1/secrets/"+url.PathEscape(name), nil, &out)
}

func (c *Client) ListSecrets() ([]SecretInfo, error) {
	var out []SecretInfo
	return out, c.do("GET", "/api/v1/secrets", nil, &out)
}

// DeleteSecret removes a secret; deleting an already-removed secret is a
// no-op (SB-153: idempotent in effect).
func (c *Client) DeleteSecret(name string) error {
	return c.do("DELETE", "/api/v1/secrets/"+url.PathEscape(name), nil, nil)
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

// DeleteTag removes the tag ref; the blob it pointed at becomes
// unreferenced and is reclaimed by the next GC.
func (c *Client) DeleteTag(name string) error {
	return c.do("DELETE", "/api/v1/tags/"+url.PathEscape(name), nil, nil)
}

// ---- File copy and fetch ----

// CopyFile copies a file (or a directory subtree) from one commit into an
// open commit at dstPath. The destination must not already exist in the
// destination commit's view unless overwrite is set, which replaces its
// accumulated content (SB-156, FS-3).
func (c *Client) CopyFile(dstCommit, dstPath, srcCommit, srcPath string, overwrite bool) error {
	q := ""
	if overwrite {
		q = "?overwrite=1"
	}
	return c.do("POST", "/api/v1/commits/"+url.PathEscape(dstCommit)+"/files/"+url.PathEscape(dstPath)+q,
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

// TxOpView summarizes one staged transaction operation.
type TxOpView struct {
	Kind     string `json:"kind"` // create | update
	Pipeline string `json:"pipeline"`
}

// Transaction is a transaction's id with its staged operations (open by
// construction: a finished transaction is removed).
type Transaction struct {
	ID  string     `json:"id"`
	Ops []TxOpView `json:"ops,omitempty"`
}

// ListTransactions returns every open transaction.
func (c *Client) ListTransactions() ([]Transaction, error) {
	var out []Transaction
	return out, c.do("GET", "/api/v1/transactions", nil, &out)
}

// InspectTransaction returns one transaction with its staged operations.
func (c *Client) InspectTransaction(tx string) (Transaction, error) {
	var out Transaction
	return out, c.do("GET", "/api/v1/transactions/"+url.PathEscape(tx), nil, &out)
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
		_ = json.NewDecoder(resp.Body).Decode(&e)
		return nil, &Error{Status: resp.StatusCode, Message: e.Error}
	}
	return resp.Body, nil
}
