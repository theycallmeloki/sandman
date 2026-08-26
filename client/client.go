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
//     value is the absolute path to that input's data directory.
//     The name must be a valid shell identifier ([A-Za-z_][A-Za-z0-9_]*),
//     because it is used verbatim as an environment variable name.
//   - The job receives <NAME>_COMMIT = id of the input revision, OUT = path
//     to the output directory, JOB_ID = job id, OUTPUT_COMMIT = output
//     commit id. These four names are reserved: a custom
//     environment variable may not shadow them. The output directory name
//     "out" is reserved and cannot be used as an input name.
//   - A pipeline with no command and no stdin runs a default entry point
//     that copies every input file to the output unchanged. A
//     pipeline with no command but with stdin is accepted at creation and
//     transitions to the failure state.
//   - An input's glob is required at creation and selects
//     which files of the input revision the job sees.
//   - A commit finished with the empty flag is explicitly empty: parent
//     content is not readable through it, even at the branch head.
//   - A pipeline created after its input history exists processes the
//     branch head once, in one output commit; it does not
//     replay older history. A stopped pipeline ignores new commits; on
//     start it processes the backlog of commits finished while stopped.
//   - Writing a file into an open commit at a path already written in that
//     same commit is rejected. Copying into a path that already
//     exists in the commit's view (including ancestors) is rejected. Jobs
//     are exempt: they upload into fresh output commits.
//   - Job inspection accepts a job id or an output commit id.
//     Job listing filters by pipeline, by produced output commit, and by an
//     inclusive set of states; a full listing additionally carries each
//     job's transform and input spec.
//   - Tags are durable global names bound to file content: put, get, and
//     list round-trip bytes exactly.
package client

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
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
	return c.doClient(c.hc, method, p, in, out)
}

// doClient is do with a specific HTTP client (a long-poll needs a client
// timeout that outlives the server's own deadline).
// decodeError converts a non-2xx response into an Error. Handler errors
// are {"error": msg} JSON; mux-generated 404/405 and any other non-JSON
// body fall back to the raw text so callers print a readable message
// instead of an empty one — the version-skew case (a new CLI hitting an
// old daemon's missing endpoint) depends on this.
func decodeError(status int, body []byte) *Error {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error != "" {
		return &Error{Status: status, Message: e.Error}
	}
	msg := strings.TrimSpace(string(body))
	if msg == "" {
		msg = http.StatusText(status)
	}
	return &Error{Status: status, Message: msg}
}

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
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return decodeError(resp.StatusCode, b)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// maxFetchBytes is the largest response body the client will read into
// memory (or stream) from a raw GET: the server's own file/tag cap is
// 1 GiB, so anything past it is a misbehaving peer, not a legitimate
// payload. Bounds GetFile/GetTag, whose buffered paths would otherwise
// allocate without limit.
const maxFetchBytes = 1 << 30

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
	b, err := readCapped(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, decodeError(resp.StatusCode, b)
	}
	return b, nil
}

// readCapped drains r up to maxFetchBytes; a larger body is an error, not
// an allocation (the 1 GiB server cap is the client's ceiling too).
func readCapped(r io.Reader) ([]byte, error) { return readCappedLimit(r, maxFetchBytes) }

func readCappedLimit(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("response exceeds the %d byte limit", limit)
	}
	return b, nil
}

// copyCapped streams r into w up to limit; a larger body is an error.
func copyCapped(w io.Writer, r io.Reader, limit int64) error {
	lr := &io.LimitedReader{R: r, N: limit + 1}
	if _, err := io.Copy(w, lr); err != nil {
		return err
	}
	if lr.N == 0 {
		return fmt.Errorf("file exceeds the %d byte limit", limit)
	}
	return nil
}

// doRaw is do with a raw byte body and raw byte response.

type Repo struct {
	Name      string   `json:"name"`
	SizeBytes uint64   `json:"sizeBytes"` // total size of files at the head of the primary branch
	Branches  []string `json:"branches,omitempty"`
}

func (c *Client) CreateRepo(name string) error {
	return c.do("POST", "/api/v1/repos", map[string]string{"name": name}, nil)
}

// DeleteRepo removes a repository. force bypasses the guard that protects
// a pipeline's output repository from accidental deletion; the internal
// spec repository is protected unconditionally.
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
// DAG: the commit's record, every downstream commit whose provenance
// includes it, and the jobs that consumed them; surviving commits' parent
// links are repaired and branch heads that pointed at a removed commit
// move to the nearest surviving ancestor or disappear. Ref is a commit id
// or repo@branch (the branch head). Deleting a branch head supersedes an
// in-flight job processing it.
func (c *Client) DeleteCommit(ref string) error {
	return c.do("DELETE", "/api/v1/commits/"+url.PathEscape(ref), nil, nil)
}

func (c *Client) HeadCommit(repo, branch string) (Commit, error) {
	var out Commit
	return out, c.do("GET", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch)+"/head", nil, &out)
}

// CreateBranch points a branch at an existing commit — creating the branch
// or retargeting it. A pipeline triggers only on commits on the branches
// it watches, and writes its output commits to its configured output
// branch (default "master"); because the output lands on a possibly
// non-watched branch, a downstream pipeline that watches the default
// branch does not run until that branch is promoted onto the output
// commit via this retargeting — scheduling keys off branch pointers
// rather than commit existence. Pipelines watching the branch process the
// commit exactly once.
func (c *Client) CreateBranch(repo, branch, head string) error {
	return c.do("POST", "/api/v1/repos/"+url.PathEscape(repo)+"/branches/"+url.PathEscape(branch), map[string]string{"head": head}, nil)
}

// Branch is a named pointer into a repo's commit history: Head is the id
// of the commit the branch currently points at.
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

// CommitHistory walks the branch's commit chain oldest-first. The walk
// is brief-inspect (the server skips its per-commit subvenant scan, which
// is O(all commits) each — the unconditional form would make a commit
// list O(depth x total) of server work), and cycle-guarded: a corrupt
// parent chain (a record whose parent points back into the visited set)
// stops the walk instead of spinning forever.
func (c *Client) CommitHistory(repo, branch string) ([]Commit, error) {
	head, err := c.HeadCommit(repo, branch)
	if err != nil {
		return nil, err
	}
	var chain []Commit
	seen := map[string]bool{}
	for cur := &head; cur != nil && cur.ID != ""; {
		if seen[cur.ID] {
			return nil, fmt.Errorf("commit history of %s@%s: corrupt parent cycle at %s", repo, branch, cur.ID)
		}
		seen[cur.ID] = true
		chain = append(chain, *cur)
		if cur.ParentID == "" {
			break
		}
		next, err := c.inspectCommitBrief(cur.ParentID)
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

// inspectCommitBrief is InspectCommit without the server's subvenant
// scan (the walkers' per-commit hot path).
func (c *Client) inspectCommitBrief(id string) (Commit, error) {
	var out Commit
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(id)+"?brief=1", nil, &out)
}

// ---- Execution hosts ----

// GpuInfo is one GPU an execution host advertises for allocation: the
// device index (as docker/CUDA enumerate it), the vendor-reported name,
// and its total memory. Busy is computed by the control plane at list
// time — the device is currently allocated to a running job.
type GpuInfo struct {
	Index     int    `json:"index"`
	Name      string `json:"name,omitempty"`
	MemoryMiB int64  `json:"memoryMiB,omitempty"`
	Busy      bool   `json:"busy,omitempty"`
}

// HostInfo is one registered execution host as the control plane sees it:
// name, exec endpoint, placement labels, advertised GPUs, last heartbeat.
type HostInfo struct {
	Name   string    `json:"name"`
	Addr   string    `json:"addr"`
	Labels []string  `json:"labels,omitempty"`
	Gpus   []GpuInfo `json:"gpus,omitempty"`
	Seen   string    `json:"seen"`
}

// Hosts lists the execution hosts currently registered with the control
// plane (the authoritative fleet view the dashboard renders).
func (c *Client) Hosts() ([]HostInfo, error) {
	var out []HostInfo
	return out, c.do("GET", "/api/v1/hosts", nil, &out)
}

// ---- Files ----

type FileInfo struct {
	Path string `json:"path"`
	Size uint64 `json:"size"`
	// Hash is the hex sha256 of the file content (the datum identifier for
	// log filters).
	Hash string `json:"hash,omitempty"`
}

func (c *Client) PutFile(commitID, p string, data []byte) error {
	_, err := c.doRaw("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), data)
	return err
}

// PutFileStream uploads a file's content from r without buffering it
// client-side: the request body streams (chunked), so a large local file
// never sits in memory twice. overwrite has the same meaning as
// PutFileOverwrite — replace content accumulated at the path.
func (c *Client) PutFileStream(commitID, p string, overwrite bool, r io.Reader) error {
	u := "/api/v1/commits/" + url.PathEscape(commitID) + "/files/" + url.PathEscape(p)
	if overwrite {
		u += "?overwrite=1"
	}
	req, err := http.NewRequest("PUT", c.base+u, r)
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return decodeError(resp.StatusCode, b)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	return err
}

// PutFileOverwrite writes a file replacing any content accumulated at the
// path: the prior content — in this commit or its ancestry — is
// removed and the new bytes become the entire content at this revision.
func (c *Client) PutFileOverwrite(commitID, p string, data []byte) error {
	_, err := c.doRaw("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+"?overwrite=1", data)
	return err
}

// PutFileSplit uploads data split into records at delimiter: each record
// is stored at p/<index>. With header, the first chunk is the
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

// PutFileURL fetches a file from an HTTP URL and stores it in the commit.
func (c *Client) PutFileURL(commitID, p, u string) error {
	return c.do("PUT", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+"?fetch="+url.QueryEscape(u), nil, nil)
}

// DeleteFile tombstones a path in an open commit: the path is removed from
// the branch's view at this revision.
func (c *Client) DeleteFile(commitID, p string) error {
	return c.do("DELETE", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), nil, nil)
}

func (c *Client) GetFile(commitID, p string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p), nil)
}

// ServiceInfo is a service pipeline's running record. A service pipeline
// runs one long-lived process that stays up and serves the materialized
// input over HTTP: the external port is bound on the control plane and
// proxied to the process's internal port, the control-plane API exposes a
// per-pipeline proxy route returning the same content as the direct
// endpoint, user annotations are preserved alongside the system's
// pipelineName annotation, and new input revisions are re-served through
// the same endpoint without restarting the process; reachability
// converges as the process comes up. The record carries the declared
// ports and the endpoint's annotations — the user's own plus the system's
// identifying pipelineName annotation.
type ServiceInfo struct {
	Pipeline    string            `json:"pipeline"`
	Internal    int               `json:"internalPort"`
	External    int               `json:"externalPort"`
	Annotations map[string]string `json:"annotations"`
}

// InspectService returns a service pipeline's running record.
func (c *Client) InspectService(pipeline string) (ServiceInfo, error) {
	var out ServiceInfo
	return out, c.do("GET", "/api/v1/services/"+url.PathEscape(pipeline), nil, &out)
}

// ServiceProxy fetches a path through the control plane's per-pipeline
// service proxy: the request is forwarded to the service's own endpoint,
// so the response is identical to the direct one.
func (c *Client) ServiceProxy(pipeline, p string) ([]byte, error) {
	return c.doRaw("GET", "/api/v1/services/"+url.PathEscape(pipeline)+"/"+url.PathEscape(p), nil)
}

func (c *Client) ListFiles(commitID string) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files", nil, &out)
}

// ListFilesGlob lists a commit's files whose paths start with the given
// prefix ("1*" lists the paths beginning with "1"). The
// server applies the filter; only prefix patterns are supported.
func (c *Client) ListFilesGlob(commitID, glob string) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files?glob="+url.QueryEscape(glob), nil, &out)
}

// ListFileHistory lists the revisions of a path across the commit's
// ancestry: one FileInfo per ancestor revision where the path resolves,
// newest first, capped at a limit (a negative limit returns every
// revision). It must not fail on outputs produced from multi-commit
// cross inputs — a cross's multi-commit provenance is just more ancestry
// to walk.
func (c *Client) ListFileHistory(commitID, p string, limit int) ([]FileInfo, error) {
	var out []FileInfo
	return out, c.do("GET", "/api/v1/commits/"+url.PathEscape(commitID)+"/files/"+url.PathEscape(p)+
		fmt.Sprintf("?history=1&limit=%d", limit), nil, &out)
}

// ---- Pipelines ----

// Transform is the execution description of a pipeline. Image is the
// container image (empty means "alpine"); when Cmd is empty the pipeline
// runs the default entry point (see the package comment). AcceptReturnCode
// declares one non-zero exit code that is treated as success.
// DatumTries retries a failing datum that many times before it is marked
// failed; a value of zero means one attempt. ErrCmd is a secondary
// command run when the primary fails for a datum: if it succeeds the
// datum is recovered, not failed. DatumTimeout and JobTimeout bound a
// datum's and a whole job's execution.
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
	// PodSpec is a full execution-environment customization document: a
	// JSON object {"env": {...}, "volumes": {...}, "workdir": ...}
	// applied to every execution participant. PodPatch is a JSON
	// modification list (RFC 6902 operations) applied to the document
	// after the spec. Both are validated as JSON at creation; malformed
	// customization fails pipeline creation.
	PodSpec  string `json:"podSpec,omitempty"`
	PodPatch string `json:"podPatch,omitempty"`
	// Secrets binds named secrets from the system's secret store into
	// every job's environment: a mount's Key is written as a file at
	// MountPath/<key> and/or injected as the EnvVar.
	// Each reference names an existing secret — the binding is explicit.
	Secrets []SecretMount `json:"secrets,omitempty"`
	// ResourceRequests and ResourceLimits declare the execution
	// environment's resources: Memory as a size
	// string ("100M"), CPU as a fractional core count, Disk as a size
	// string. Sandbox deviation: docker expresses a CPU request only as
	// an allocation, and a disk request is recorded but not enforceable
	// on docker's default driver.
	ResourceRequests *ResourceRequests `json:"resourceRequests,omitempty"`
	ResourceLimits   *ResourceLimits   `json:"resourceLimits,omitempty"`
}

// ResourceRequests are the pipeline's requested resources.
type ResourceRequests struct {
	Memory string  `json:"memory,omitempty"`
	CPU    float64 `json:"cpu,omitempty"`
	Disk   string  `json:"disk,omitempty"`
	// GPU is the number of GPUs each datum worker requires (0 = none).
	// GPU work is never "all GPUs by default": the control plane places
	// it on a registered execution host advertising enough free devices
	// and allocates specific device indices per job, so parallel jobs
	// (and parallel datum workers) each get their own device. A pipeline
	// requesting GPUs with no satisfiable host surfaces the outage as
	// the crashed state, exactly like an unsatisfiable placement label.
	GPU int `json:"gpu,omitempty"`
}

// ResourceLimits are the pipeline's hard resource limits.
type ResourceLimits struct {
	Memory string  `json:"memory,omitempty"`
	CPU    float64 `json:"cpu,omitempty"`
}

// Input is a file-scoped input: files of the repo matched by Glob.
// Name aliases the input's environment variable and defaults to the repo
// name; it must be a valid shell identifier and not "out".
// Branch defaults to "master". Cross, when non-empty, makes the input the
// cartesian product of the listed inputs (each member is itself a
// file-scoped input with its own repo, glob, branch, and name); a
// file-scoped Input with Cross set is invalid.
// Trigger is a size-based commit trigger: bytes newly committed
// to the watched branch accumulate, and when the accumulated total
// reaches SizeBytes the pipeline runs on the accumulated data. The input
// reads its dedicated accumulation branch (derived from the pipeline and
// the input's position), not the watched branch.
type Trigger struct {
	SizeBytes int64 `json:"sizeBytes,omitempty"`
	// Branch is the watched branch (default "master").
	Branch string `json:"branch,omitempty"`
}

// GitInput declares an external git repository as a pipeline input: the
// repository at URL is mapped to an auto-created repository named by the
// input's own Name or derived from the URL when the input has no name,
// and a push event for URL on the tracked
// branch commits the pushed revision into that repository and triggers
// the pipeline.
type GitInput struct {
	URL string `json:"url"`
	// Branch is the tracked branch (default "master"): pushes to any
	// other branch are ignored entirely.
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
	// containing one file per member. Outer marks a member whose
	// unmatched keys still produce datums, each carrying only that
	// member's file — the unmatched members' directories are absent.
	// A file-scoped Input with Join set is invalid.
	Join   []Input `json:"join,omitempty"`
	JoinOn string  `json:"joinOn,omitempty"`
	Outer  bool    `json:"outer,omitempty"`
	// Group, when non-empty, makes the input a group: files across all
	// members are collected by their GroupBy capture value into one datum
	// per key (union, not cross product). A member with JoinOn
	// joins first, then groups the joined pairs.
	Group   []Input `json:"group,omitempty"`
	GroupBy string  `json:"groupBy,omitempty"`
	// Union, when non-empty, makes the input a union: the members'
	// (branches') files are exposed under this input's namespace, and
	// files at the same path from different branches merge by
	// concatenation in branch order — one datum per distinct path.
	// A member may be a plain repo, a cross, or a nested
	// union; the exposed namespace is the member's own Name.
	Union []Input `json:"union,omitempty"`
	// Cron makes the input a scheduled cron input: the
	// schedule is an "@every <duration>" spec, and at each tick the
	// system commits a file named by the tick time (UTC RFC3339) to the
	// input's auto-created repository (named after the pipeline and the
	// input). Overwrite makes each tick's commit replace the previous
	// tick's file instead of accumulating. A cron input needs no repo or
	// glob.
	Cron      string `json:"cron,omitempty"`
	Overwrite bool   `json:"overwrite,omitempty"`
	// Git makes the input a git input: the external repository at URL is
	// mapped to an auto-created repository named by the input's own Name
	// or derived from the URL when unnamed;
	// a push event for URL on the tracked branch (Branch, default
	// "master") commits the pushed revision into the mapped repository
	// and triggers the pipeline. A git input needs no repo or glob.
	Git *GitInput `json:"git,omitempty"`
	// Trigger makes the input a size trigger: bytes newly
	// committed to the watched branch accumulate, and every completed
	// threshold unit runs the pipeline on the accumulated data, from the
	// input's dedicated accumulation branch.
	Trigger *Trigger `json:"trigger,omitempty"`
	// Lazy marks the input as materialized on demand rather than eagerly.
	// The flag is part of the pipeline spec and is recorded on every job's
	// input snapshot; sandman materializes the files for execution either
	// way.
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

// ChunkSpec configures how a side's glob matches are grouped into datums:
// either a target number of datums or a target chunk size. It is
// an internal scheduling knob — the output must be identical regardless.
type ChunkSpec struct {
	Number    int `json:"number,omitempty"`
	SizeBytes int `json:"sizeBytes,omitempty"`
}

// WorkerStatus is one worker's live state while its job runs: the datum
// it is processing, when it started, and how many datums are
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
	// a value of zero means a queue of one. Autoscaling derives the
	// execution scale from the datum count, capped at the configured
	// parallelism (the cap is never exceeded, and never more workers than
	// datums).
	MaxQueueSize int  `json:"maxQueueSize,omitempty"`
	Autoscaling  bool `json:"autoscaling,omitempty"`
	// Standby pipelines idle in the standby state and activate only when
	// work arrives, returning to standby once it settles.
	Standby bool `json:"standby,omitempty"`
	// OutputBranch names the branch the pipeline writes its output to
	// (default "master"); a downstream pipeline that watches a different
	// branch of the output repo does not run until that branch is pointed
	// at the output commit.
	OutputBranch string `json:"outputBranch,omitempty"`
	// Reprocess is a persisted spec field: every job re-executes all of
	// its datums instead of skipping datums unchanged from a previous
	// successful run (an update that sets it requests full reprocessing).
	// EnableStats persists per-datum statistics: a stats branch on the
	// output repo and inspectable datum records; it is one-way — an
	// update cannot disable it. Update is a request flag (create when
	// absent).
	Update      bool `json:"update,omitempty"`
	Reprocess   bool `json:"reprocess,omitempty"`
	EnableStats bool `json:"enableStats,omitempty"`
	// Placement names the placement label a host must bear for this
	// pipeline's jobs to run. An operator designates execution hosts
	// with placement labels, and a host joins the cluster by registering
	// with the control plane using configuration set at host setup time;
	// the control plane schedules labeled work onto a registered host
	// bearing the label — the pipeline definition never enumerates a
	// host address or identity, and a job completes only once a host
	// bearing the required label is available, its output provably
	// coming from that host's execution. Empty means unconstrained —
	// the control plane's own executor runs the jobs (unplaceable work
	// surfaces as the crashed pipeline state until a host returns).
	Placement string `json:"placement,omitempty"`
	// Framework names an execution framework; sandman supports none, so
	// any non-empty value is rejected at creation naming the framework.
	Framework string `json:"framework,omitempty"`
	// Spout, when set, makes the pipeline a spout: it must not
	// declare an input, its transform runs in the background, and the
	// daemon commits each data-bearing cycle to the output branch.
	Spout *Spout `json:"spout,omitempty"`
	// Service, when set, makes the pipeline a service: instead
	// of settling after processing a revision, one long-running process
	// serves the pipeline's input over HTTP, and new input revisions are
	// served automatically without redeploying. InternalPort is where the
	// user's process listens inside the execution environment;
	// ExternalPort is bound on the control-plane host and proxied to the
	// process wherever it runs: even when the process runs on a remote
	// execution host, the control plane forwards traffic arriving at its
	// external port to the remote worker's internal port, so clients only
	// ever need the control-plane host's address, reachability converges
	// as the worker's process comes up, and the response is the exact
	// served file content from the pipeline's input data. Annotations are
	// preserved on the service record alongside the system's own
	// pipelineName annotation.
	Service *Service `json:"service,omitempty"`
	// Egress, when set, copies the job's finished output to an external
	// destination after the output commit succeeds; a failed egress write
	// fails the job with an egress-related reason even though the output
	// commit itself succeeded.
	Egress *Egress `json:"egress,omitempty"`
}

// SecretMount references one secret's key into the execution
// environment: MountPath mounts the key's value as a file at
// MountPath/<key> (paths must live under /sandman/ — the execution
// namespace); EnvVar injects the key's value as that environment
// variable. At least one of the two must be set.
type SecretMount struct {
	Name      string `json:"name"`
	Key       string `json:"key,omitempty"`
	MountPath string `json:"mountPath,omitempty"`
	EnvVar    string `json:"envVar,omitempty"`
}

// Egress is a pipeline's external output destination. URL names the
// destination; file:// destinations copy the output files
// into the target directory, and any other scheme is refused at egress
// time, failing the job.
type Egress struct {
	URL string `json:"url"`
}

// Service is a service pipeline's declaration.
type Service struct {
	InternalPort int               `json:"internalPort"`
	ExternalPort int               `json:"externalPort"`
	Annotations  map[string]string `json:"annotations,omitempty"`
}

// Spout is a spout pipeline's options: Overwrite replaces the
// committed file content each cycle instead of accumulating it, and
// Marker names a directory whose files are committed to a separate marker
// branch (a name with glob metacharacters is rejected).
type Spout struct {
	Overwrite bool   `json:"overwrite,omitempty"`
	Marker    string `json:"marker,omitempty"`
}

// Pipeline state machine: running, stopped, standby, failure, crashed. Stopped is a persistent flag distinct from the transient state:
// stopping a pipeline sets Stopped and reports state "paused".
// Transform and Input are populated in full inspections; the other fields
// are the current version's state.
type PipelineInfo struct {
	Name         string        `json:"name"`
	State        string        `json:"state"`
	Reason       string        `json:"reason,omitempty"`
	Description  string        `json:"description,omitempty"`
	Stopped      bool          `json:"stopped"`
	Version      int           `json:"version,omitempty"`
	Transform    *Transform    `json:"transform,omitempty"`
	Input        *Input        `json:"input,omitempty"`
	Parallelism  *Parallelism  `json:"parallelism,omitempty"`
	ChunkSpec    *ChunkSpec    `json:"chunkSpec,omitempty"`
	MaxQueueSize int           `json:"maxQueueSize,omitempty"`
	Autoscaling  bool          `json:"autoscaling,omitempty"`
	Standby      bool          `json:"standby,omitempty"`
	OutputBranch string        `json:"outputBranch,omitempty"`
	Reprocess    bool          `json:"reprocess,omitempty"`
	EnableStats  bool          `json:"enableStats,omitempty"`
	Spout        *Spout        `json:"spout,omitempty"`
	Service      *Service      `json:"service,omitempty"`
	Egress       *Egress       `json:"egress,omitempty"`
	Secrets      []SecretMount `json:"secrets,omitempty"`
	Placement    string        `json:"placement,omitempty"`
	// JobCounts maps every job state to the number of that pipeline's
	// jobs currently in that state, derived live from the job records
	// and keyed strictly by state — a successful job increments only the
	// success bucket and never bleeds into any other.
	JobCounts map[string]int `json:"jobCounts,omitempty"`
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
// pipelines whose definition is lost, as name-only entries.
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
// the current version, 1 the previous, up to the oldest.
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

// RunPipeline manually triggers a pipeline run. Provenance, when
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
// pipeline: a commit lands now regardless of the schedule, and
// scheduled ticks keep flowing.
func (c *Client) TriggerCron(name string) error {
	return c.do("POST", "/api/v1/pipelines/"+url.PathEscape(name)+"/trigger", nil, nil)
}

// PushGitEvent delivers a git push event: the daemon
// commits the pushed revision into every mapped repository bound to the
// URL (on the tracked branch) and triggers the consuming pipelines.
// files is the pushed revision's working tree (the external repository's
// content); private marks a repository the system cannot clone (no
// credentials) — the event is accepted but produces no commit and the
// bound pipelines fail with a reason naming the URL.
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
// reads, one per input side. DatumFile names the input side and the file
// within it.
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
// sides' current finished heads, without creating or running a pipeline:
// each side's glob matches are enumerated and combined as the Cartesian
// product, so two inputs of 5 files each yield 25 datums; a side with no
// finished head contributes nothing, so the set is empty.
func (c *Client) EnumerateDatums(input Input) ([]Datum, error) {
	var out []Datum
	return out, c.do("POST", "/api/v1/datums", map[string]any{"input": input}, &out)
}

// DatumInfo is one datum's record for a job: identity, per-side input
// files, the produced output files, outcome state (running | success |
// recovered | failed | skipped), process time, and timing.
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

// DatumPage is a paginated datum listing: the page's datums
// plus the total page count and the served (zero-based) page index.
type DatumPage struct {
	Datums     []DatumInfo `json:"datums"`
	TotalPages int         `json:"totalPages"`
	Page       int         `json:"page"`
}

// ListDatums lists a job's datums, state-ordered (failed first, skipped
// last) and paginated. limit 0 requests everything; a page index at or
// beyond the page count errors.
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
// does not record per-datum statistics.
func (c *Client) InspectDatum(jobID, datumID string) (DatumInfo, error) {
	var out DatumInfo
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(jobID)+"/datums/"+url.PathEscape(datumID), nil, &out)
}

// RestartDatum aborts a datum's current processing and starts it over:
// the next status observation shows it running with a later start time,
// and the job still completes.
func (c *Client) RestartDatum(jobID, datumID string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(jobID)+"/datums/"+url.PathEscape(datumID)+"/restart", nil, nil)
}

// ---- Jobs and flush ----

// Job states: running, queued, success, failure, killed, skipped.
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
	// listings: a lightweight listing omits the spec-heavy fields while
	// core identity fields such as the pipeline name stay populated, and
	// the two modes return the same set of jobs with the same count —
	// truncation affects only the spec fields, never core identity or
	// the job count. Historical jobs keep their own version's spec even
	// after a pipeline update.
	Version   int        `json:"version,omitempty"`
	Transform *Transform `json:"transform,omitempty"`
	Input     *Input     `json:"input,omitempty"`
	// StatsCommit is the job's per-datum statistics commit on the output
	// repo's "stats" branch, when statistics are enabled.
	StatsCommit string `json:"statsCommit,omitempty"`
	// Per-datum outcome counts: processed (success), recovered
	// (primary failed, error handler succeeded), failed, skipped (datum
	// unchanged from a previous successful run).
	Processed int `json:"processed,omitempty"`
	Recovered int `json:"recovered,omitempty"`
	Failed    int `json:"failed,omitempty"`
	Skipped   int `json:"skipped,omitempty"`
	// Workers is the live per-worker status while the job runs.
	Workers []WorkerStatus `json:"workers,omitempty"`
	// Progress is a live progress snapshot computed on inspection (never
	// in lightweight listings): the job's full datum count plus current
	// done/running/queued/failed counts and the mean process time of the
	// finished datums — the dashboard's progress bar and ETA inputs.
	Progress *JobProgress `json:"progress,omitempty"`
}

// JobProgress is one job's progress snapshot (see Job.Progress). Done
// counts every terminal datum (success, recovered, failed, skipped);
// Running is the workers' active datums; the rest is Queued.
type JobProgress struct {
	Total          int     `json:"total"`
	Done           int     `json:"done"`
	Failed         int     `json:"failed"`
	Running        int     `json:"running"`
	Queued         int     `json:"queued"`
	AvgProcessTime float64 `json:"avgProcessTime,omitempty"` // seconds
}

func (c *Client) ListJobs() ([]Job, error) {
	return c.ListJobsFiltered(JobFilter{})
}

// JobFilter selects the jobs a listing returns. OutputCommit matches the
// commit a job produced; States is an inclusive set; Full includes each
// job's transform and input spec; History is the version depth: 0 =
// current version only, N = current version plus N older versions, -1 =
// every version. InputCommits returns only jobs whose input set includes
// every listed commit.
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
	// inspecting with neither a job nor an output commit identifier is
	// rejected stating that one of the two must be specified
	if id == "" {
		return out, fmt.Errorf("must specify a Job or an OutputCommit")
	}
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(id), nil, &out)
}

func (c *Client) CancelJob(id string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(id)+"/cancel", nil, nil)
}

// StopJob stops a running or merging job: the in-flight work is killed
// and the job is recorded as killed rather than waiting for it to finish.
// The stop is scoped to that job — the job for a later input commit is
// still created and runs to completion in the SUCCESS state, and job
// listing returns the newest job first. A queued job settles as killed
// when it reaches the front of the pipeline gate and never blocks the
// next job.
func (c *Client) StopJob(id string) error {
	return c.do("POST", "/api/v1/jobs/"+url.PathEscape(id)+"/stop", nil, nil)
}

func (c *Client) DeleteJob(id string) error {
	return c.do("DELETE", "/api/v1/jobs/"+url.PathEscape(id), nil, nil)
}

// Reset removes every repository, pipeline, and job. It is idempotent
// and requires healthy metadata. The yes=1 parameter is
// the destructive confirmation: the endpoint refuses without it.
func (c *Client) Reset() error {
	return c.do("POST", "/api/v1/reset?yes=1", nil, nil)
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
// (split by outcome), file writes, and job listings.
func (c *Client) FetchMetrics() (string, error) {
	b, err := c.doRaw("GET", "/api/v1/metrics", nil)
	return string(b), err
}

// CollectGarbage reclaims durable artifacts no longer referenced by any
// commit, tag, or spec record. It fails while a job is running.
func (c *Client) CollectGarbage() error {
	return c.do("POST", "/api/v1/gc", nil, nil)
}

// Check runs the control plane's consistency check: an
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
// single-side jobs.
func (c *Client) FlushSet(commitIDs []string, timeout time.Duration) ([]Job, error) {
	return c.flushSet(commitIDs, timeout)
}

// flushSet blocks on the server-side flush: the daemon runs
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

// WaitJob blocks until the job reaches a terminal state: the
// server long-polls its own state broadcasts, so the client makes one
// request. A timeout returns the job's current state with an error.
func (c *Client) WaitJob(jobID string, timeout time.Duration) (Job, error) {
	// the long-poll needs a client whose deadline outlives the server's
	// own wait deadline: equal deadlines race and the client
	// can time out "awaiting headers" just before the server's response
	hc := &http.Client{Timeout: timeout + 30*time.Second}
	var j Job
	err := c.doClient(hc, "GET", "/api/v1/jobs/"+url.PathEscape(jobID)+"/wait?timeout="+url.QueryEscape(timeout.String()), nil, &j)
	return j, err
}

// AllTerminal reports whether every job in the slice has settled: an
// empty slice is not terminal (nothing to wait on). Used by flush's
// downstream-consumer check.
func AllTerminal(jobs []Job) bool {
	if len(jobs) == 0 {
		return false
	}
	for _, j := range jobs {
		if j.State == "running" || j.State == "queued" {
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
	// is downstream of every flushed commit (wave 1: B's output is
	// downstream of A only, so it is excluded; C's pairing and D are
	// downstream of both). A single closure seeded by jobs whose inputs
	// contain every commit is wrong when the set spans a DAG: the pairing
	// job's inputs reference the upstream OUTPUT commit (B-out), not A's
	// raw commit, so no direct match exists to seed the walk.
	//
	// The walk is indexed: a commit→consuming-jobs adjacency map is built
	// once, and each queue head follows it directly instead of scanning
	// every job (the previous walk was O(J²·I) per commit — a full job
	// scan per queued head plus queue-slice shifts; with the index it is
	// O(J·I + closure size) per commit, which is what a flush's signal
	// loop pays on every state-change broadcast).
	type closure struct {
		set map[string]bool
		ord []string
	}
	byID := map[string]Job{}
	byCommit := map[string][]string{}
	for _, j := range jobs {
		byID[j.ID] = j
		for _, ic := range j.InputCommits {
			byCommit[ic] = append(byCommit[ic], j.ID)
		}
	}
	cl := make([]closure, len(commitIDs))
	for i, id := range commitIDs {
		c := closure{set: map[string]bool{}}
		seen := map[string]bool{}
		queued := map[string]bool{} // a produced commit is walked once
		var queue []string
		enqueue := func(j Job) {
			if seen[j.ID] {
				return
			}
			seen[j.ID] = true
			c.set[j.ID] = true
			c.ord = append(c.ord, j.ID)
			if j.OutputCommit != "" && !queued[j.OutputCommit] {
				queued[j.OutputCommit] = true
				queue = append(queue, j.OutputCommit)
			}
			if j.StatsCommit != "" && !queued[j.StatsCommit] {
				queued[j.StatsCommit] = true
				queue = append(queue, j.StatsCommit)
			}
		}
		for _, jid := range byCommit[id] {
			enqueue(byID[jid])
		}
		// an index cursor instead of queue[1:] slicing (a slice shift per
		// pop is quadratic in the queue length)
		for qpos := 0; qpos < len(queue); qpos++ {
			for _, jid := range byCommit[queue[qpos]] {
				j := byID[jid]
				// mirrors the original walk: a job producing no output
				// commit is not a downstream consumer
				if j.OutputCommit == "" {
					continue
				}
				enqueue(j)
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
// incarnation's job.
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

// ---- Secrets ----

// SecretInfo is a secret's inspectable record: name, type label, and the
// system-assigned creation timestamp (the data itself is write-only).
type SecretInfo struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Created string `json:"created"`
}

// CreateSecret stores a named typed metadata blob. The type is
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
// no-op (idempotent in effect).
func (c *Client) DeleteSecret(name string) error {
	return c.do("DELETE", "/api/v1/secrets/"+url.PathEscape(name), nil, nil)
}

// ---- Tags ----

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
// accumulated content.
func (c *Client) CopyFile(dstCommit, dstPath, srcCommit, srcPath string, overwrite bool) error {
	q := ""
	if overwrite {
		q = "?overwrite=1"
	}
	return c.do("POST", "/api/v1/commits/"+url.PathEscape(dstCommit)+"/files/"+url.PathEscape(dstPath)+q,
		map[string]string{"srcCommit": srcCommit, "srcPath": srcPath}, nil)
}

// FileFetch is the response of a raw file GET.
type FileFetch struct {
	Data            []byte
	ContentType     string
	ContentDisp     string // "" unless download=true
	ContentEncoding string
}

// FetchFile GETs a file's raw bytes with response headers. With download
// true the server attaches an attachment Content-Disposition. The body is
// buffered and bounded at the server's 1 GiB cap; a streaming variant
// with a per-call timeout is FetchFileTo.
func (c *Client) FetchFile(commitID, p string, download bool) (FileFetch, error) {
	var buf bytes.Buffer
	f, err := c.FetchFileTo(&buf, commitID, p, download, 0)
	f.Data = buf.Bytes()
	return f, err
}

// FetchFileTo streams a file GET's body into w, returning the response
// headers without buffering the payload (a multi-hundred-MB file never
// lives in client memory). The stream is bounded at the server's 1 GiB
// cap. timeout overrides the client's default 60s deadline per call —
// a slow large transfer aborts mid-read under the shared cap; 0 keeps
// the client default.
func (c *Client) FetchFileTo(w io.Writer, commitID, p string, download bool, timeout time.Duration) (FileFetch, error) {
	u := "/api/v1/commits/" + url.PathEscape(commitID) + "/files/" + url.PathEscape(p)
	if download {
		u += "?download=true"
	}
	hc := c.hc
	if timeout > 0 {
		cp := *c.hc
		cp.Timeout = timeout
		hc = &cp
	}
	req, err := http.NewRequest("GET", c.base+u, nil)
	if err != nil {
		return FileFetch{}, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return FileFetch{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return FileFetch{}, decodeError(resp.StatusCode, b)
	}
	if err := copyCapped(w, resp.Body, maxFetchBytes); err != nil {
		return FileFetch{}, err
	}
	return FileFetch{
		ContentType:     resp.Header.Get("Content-Type"),
		ContentDisp:     resp.Header.Get("Content-Disposition"),
		ContentEncoding: resp.Header.Get("Content-Encoding"),
	}, nil
}

// ---- Transactions ----

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
// finishes. Pipelines staged together may consume each other's output.
func (c *Client) CreatePipelineTx(p Pipeline, tx string) error {
	return c.do("POST", "/api/v1/pipelines?transaction="+url.QueryEscape(tx), p, nil)
}

// FinishTransaction applies every staged operation atomically: either all
// take effect or none do. It fails with an "outside of transaction" error
// when a staged pipeline was modified outside the transaction meanwhile.
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

// ---- Client-side detailed rendering ----

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

// DescribeJob renders a job's metadata in detailed human-readable form.
// The client renders every metadata object — repositories, output
// commits, files, pipelines, and jobs — through the Describe* helpers
// (DescribeRepo/DescribeCommit/DescribeFile/DescribePipeline/
// DescribeJob); each render completes without error over metadata
// produced by a real end-to-end run (a finished commit and a completed
// job) and returns a non-empty string. The contract is error-free
// rendering, not any particular rendered content.
func (c *Client) DescribeJob(id string) (string, error) {
	j, err := c.InspectJob(id)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Job: %s\nPipeline: %s\nState: %s\nOutputCommit: %s\nInputCommits: %v\n",
		j.ID, j.Pipeline, j.State, j.OutputCommit, j.InputCommits), nil
}

// ---- Logs ----

// LogParams selects the log lines a query returns. A datum filter narrows
// by an input file of the jobs (matched by path or by content hash, which
// is hex sha256 as returned by file listings) and requires a pipeline or
// job filter. Since is a relative window: lines older than now−Since are
// excluded. An empty filter set queries every job's logs (logs from all
// jobs are searchable globally).
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

// Backup streams the control plane's full durable state as a tar.gz into
// w: repos, tags, pipelines, jobs, dedup, logs, spout markers, secrets,
// transactions, triggers — everything under the state dir. The store part
// is captured under the store's write lock (the single-writer's buffer),
// so a restored state is a consistent point-in-time snapshot.
//
// The response is validated as a gzip stream: the server writes its
// gzip trailer only on the full success path (a mid-stream failure
// returns without closing the writers), so a missing trailer means a
// truncated archive. The raw bytes stream to w unchanged; a tee'd copy
// is inflated concurrently and must end at the trailer (checksummed) —
// otherwise Backup returns an error instead of silently landing a
// corrupt archive in w.
func (c *Client) Backup(w io.Writer) error {
	req, err := http.NewRequest("GET", c.base+"/api/v1/backup", nil)
	if err != nil {
		return err
	}
	// The shared c.hc carries a 60s total-request timeout — right for
	// JSON verbs, fatal for an archive that grows with the logs: a
	// deadline mid-transfer aborts the stream. Dedicated client with a
	// bounded connect and an unbounded transfer (flush/wait use the
	// same per-call margin pattern; backup has no natural deadline).
	hc := &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
	}}
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return decodeError(resp.StatusCode, b)
	}
	pr, pw := io.Pipe()
	validated := make(chan error, 1)
	go func() {
		gz, err := gzip.NewReader(pr)
		if err == nil {
			_, err = io.Copy(io.Discard, gz)
		}
		validated <- err
	}()
	_, copyErr := io.Copy(io.MultiWriter(w, pw), resp.Body)
	if err := pw.Close(); err != nil {
		return fmt.Errorf("backup: close validation pipe: %w", err)
	}
	gzErr := <-validated
	if copyErr != nil {
		return fmt.Errorf("backup: %w", copyErr)
	}
	if gzErr != nil {
		return fmt.Errorf("backup: truncated archive: %w", gzErr)
	}
	return nil
}
