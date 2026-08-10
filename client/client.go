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

// ---- Repositories ----

type Repo struct {
	Name      string   `json:"name"`
	SizeBytes uint64   `json:"sizeBytes"` // total size of files at the head of the primary branch
	Branches  []string `json:"branches,omitempty"`
}

func (c *Client) CreateRepo(name string) error {
	return c.do("POST", "/api/v1/repos", map[string]string{"name": name}, nil)
}

func (c *Client) DeleteRepo(name string) error {
	return c.do("DELETE", "/api/v1/repos/"+url.PathEscape(name), nil, nil)
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

// ---- Files ----

type FileInfo struct {
	Path string `json:"path"`
	Size uint64 `json:"size"`
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
// runs the default entry point (see the package comment).
type Transform struct {
	Image   string            `json:"image,omitempty"`
	Cmd     []string          `json:"cmd,omitempty"`
	Stdin   []string          `json:"stdin,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	User    string            `json:"user,omitempty"`
	Workdir string            `json:"workdir,omitempty"`
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
}

// Pipeline state machine (P7): running, stopped, standby, failure, degraded,
// crashed. Terminal failure states carry a reason.
type PipelineInfo struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Reason  string `json:"reason,omitempty"`
	Version int    `json:"version,omitempty"`
}

func (c *Client) CreatePipeline(p Pipeline) error {
	return c.do("POST", "/api/v1/pipelines", p, nil)
}

func (c *Client) InspectPipeline(name string) (PipelineInfo, error) {
	var out PipelineInfo
	return out, c.do("GET", "/api/v1/pipelines/"+url.PathEscape(name), nil, &out)
}

func (c *Client) ListPipelines() ([]PipelineInfo, error) {
	var out []PipelineInfo
	return out, c.do("GET", "/api/v1/pipelines", nil, &out)
}

func (c *Client) DeletePipeline(name string) error {
	return c.do("DELETE", "/api/v1/pipelines/"+url.PathEscape(name), nil, nil)
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
}

func (c *Client) ListJobs() ([]Job, error) {
	var out []Job
	return out, c.do("GET", "/api/v1/jobs", nil, &out)
}

func (c *Client) InspectJob(id string) (Job, error) {
	var out Job
	return out, c.do("GET", "/api/v1/jobs/"+url.PathEscape(id), nil, &out)
}

// Flush waits until every job triggered by the commit is terminal and
// returns them. It polls the job list; a timeout returns the jobs seen so
// far with their states as the error.
func (c *Client) Flush(commitID string, timeout time.Duration) ([]Job, error) {
	deadline := time.Now().Add(timeout)
	for {
		jobs, err := c.ListJobs()
		if err != nil {
			return jobs, err
		}
		relevant := make([]Job, 0, len(jobs))
		allTerminal := true
		for _, j := range jobs {
			for _, ic := range j.InputCommits {
				if ic == commitID {
					relevant = append(relevant, j)
				}
			}
		}
		for _, j := range relevant {
			if j.State == "running" {
				allTerminal = false
			}
		}
		if len(relevant) > 0 && allTerminal {
			return relevant, nil
		}
		if time.Now().After(deadline) {
			return relevant, fmt.Errorf("flush timeout: %d job(s) not terminal", len(relevant))
		}
		time.Sleep(250 * time.Millisecond)
	}
}
