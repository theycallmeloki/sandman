package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"sandman/client"
	"sandman/internal/store"
)

// MCP / JSON-RPC 2.0 error codes
const (
	mcpErrCodeParseError     = -32700
	mcpErrCodeInvalidRequest = -32600
	mcpErrCodeMethodNotFound = -32601
	mcpErrCodeInvalidParams  = -32602
	mcpErrCodeInternal       = -32603
)

// MCP Protocol version
const mcpProtocolVersion = "2024-11-05"

// mcpBlockingToolBudget bounds how long a tool call may block on daemon work
// that waits for in-flight jobs to settle. The daemon allows such work
// cancelSettleBudget (30s) so that a wedged job cannot hold an HTTP handler
// forever, but an MCP client -- usually a model on a much shorter deadline --
// must not be held anywhere near that long. The work is durable before the
// waiting starts (stopPipeline records the stop before cancelling its jobs),
// so abandoning the wait still leaves the call applied.
const mcpBlockingToolBudget = 3 * time.Second

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type mcpTool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type mcpContent struct {
	Type string `json:"type"` // "text"
	Text string `json:"text"`
}

type mcpToolResult struct {
	Content []mcpContent `json:"content"`
	IsError bool         `json:"isError,omitempty"`
}

type mcpInitializeParams struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	ClientInfo      struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"clientInfo"`
}

type mcpInitializeResult struct {
	ProtocolVersion string         `json:"protocolVersion"`
	Capabilities    map[string]any `json:"capabilities"`
	ServerInfo      struct {
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"serverInfo"`
	Instructions string `json:"instructions,omitempty"`
}

type mcpCallToolParams struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments"`
}

type mcpSession struct {
	id        string
	createdAt time.Time
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

// runBoundedTool runs fn, which may wait on job settlement, and abandons only
// the wait after mcpBlockingToolBudget. It reports whether the wait was
// abandoned. An error fn returns promptly -- an unknown pipeline, say -- is
// still returned, because that validation happens before the waiting begins.
func runBoundedTool(fn func() error) (settling bool, err error) {
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return false, err
	case <-time.After(mcpBlockingToolBudget):
		return true, nil
	}
}

func newMCPSession(id string) *mcpSession {
	return &mcpSession{
		id:        id,
		createdAt: time.Now(),
		send:      make(chan []byte, 128),
		done:      make(chan struct{}),
	}
}

func (s *mcpSession) close() {
	s.closeOnce.Do(func() {
		close(s.done)
	})
}

func (s *mcpSession) enqueue(msg []byte) bool {
	select {
	case <-s.done:
		return false
	case s.send <- msg:
		return true
	default:
		select {
		case <-s.done:
			return false
		case s.send <- msg:
			return true
		case <-time.After(3 * time.Second):
			return false
		}
	}
}

type mcpToolHandler func(args map[string]any) (*mcpToolResult, error)

type mcpServer struct {
	d        *daemon
	mu       sync.RWMutex
	sessions map[string]*mcpSession
	tools    []mcpTool
	handlers map[string]mcpToolHandler
}

func newMCPServer(d *daemon) *mcpServer {
	s := &mcpServer{
		d:        d,
		sessions: make(map[string]*mcpSession),
		handlers: make(map[string]mcpToolHandler),
	}
	s.registerTools()
	return s
}

func randomSessionID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

func (s *mcpServer) createSession(id string) *mcpSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := newMCPSession(id)
	s.sessions[id] = sess
	return sess
}

func (s *mcpServer) getSession(id string) *mcpSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

func (s *mcpServer) removeSession(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sess, ok := s.sessions[id]; ok {
		sess.close()
		delete(s.sessions, id)
	}
}

func (s *mcpServer) sessionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sessions)
}

func (s *mcpServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	sessionID := randomSessionID()
	session := s.createSession(sessionID)
	defer s.removeSession(sessionID)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})
	_ = rc.SetReadDeadline(time.Time{})

	base := strings.TrimSuffix(r.URL.Path, "/mcp/sse")
	base = strings.TrimSuffix(base, "/mcp/sse/")
	base = strings.TrimSuffix(base, "/")
	msgURL := fmt.Sprintf("%s/mcp/messages?sessionId=%s", base, sessionID)

	fmt.Fprintf(w, "event: endpoint\ndata: %s\n\n", msgURL)
	flusher.Flush()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-session.done:
			return
		case msg, ok := <-session.send:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

func (s *mcpServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed: use POST for /mcp/messages")
		return
	}

	sessionID := r.URL.Query().Get("sessionId")
	if sessionID == "" {
		sessionID = r.URL.Query().Get("session_id")
	}
	if sessionID == "" {
		sessionID = r.Header.Get("X-Session-ID")
	}
	if sessionID == "" {
		writeErr(w, http.StatusBadRequest, "missing sessionId query parameter")
		return
	}

	session := s.getSession(sessionID)
	if session == nil {
		writeErr(w, http.StatusNotFound, fmt.Sprintf("session %q not found or expired", sessionID))
		return
	}

	body, err := readBody(r.Body, 16<<20)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}

	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) == 0 {
		writeErr(w, http.StatusBadRequest, "empty request body")
		return
	}

	// Check for batch requests vs single request
	if strings.HasPrefix(trimmed, "[") {
		var reqs []jsonRPCRequest
		if err := json.Unmarshal([]byte(trimmed), &reqs); err != nil {
			errResp := jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      json.RawMessage("null"),
				Error:   &jsonRPCError{Code: mcpErrCodeParseError, Message: "Parse error: " + err.Error()},
			}
			b, _ := json.Marshal(errResp)
			session.enqueue(b)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			w.Write(b)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("mcp: panic in batch request handler: %v", r)
				}
			}()
			for _, req := range reqs {
				if len(req.ID) == 0 || string(req.ID) == "null" {
					s.handleNotification(req)
				} else {
					resp := s.dispatchRequest(req)
					b, _ := json.Marshal(resp)
					session.enqueue(b)
				}
			}
		}()
		return
	}

	var req jsonRPCRequest
	if err := json.Unmarshal([]byte(trimmed), &req); err != nil {
		errResp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      json.RawMessage("null"),
			Error:   &jsonRPCError{Code: mcpErrCodeParseError, Message: "Parse error: " + err.Error()},
		}
		b, _ := json.Marshal(errResp)
		session.enqueue(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(b)
		return
	}

	if req.JSONRPC == "" {
		req.JSONRPC = "2.0"
	}
	if req.JSONRPC != "2.0" {
		errResp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &jsonRPCError{Code: mcpErrCodeInvalidRequest, Message: "Invalid JSON-RPC version: " + req.JSONRPC},
		}
		b, _ := json.Marshal(errResp)
		session.enqueue(b)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		w.Write(b)
		return
	}

	isNotification := len(req.ID) == 0 || string(req.ID) == "null"
	w.WriteHeader(http.StatusAccepted)

	if isNotification {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("mcp: panic in notification handler: %v", r)
				}
			}()
			s.handleNotification(req)
		}()
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("mcp: panic in request handler: %v", r)
				errResp := jsonRPCResponse{
					JSONRPC: "2.0",
					ID:      req.ID,
					Error:   &jsonRPCError{Code: mcpErrCodeInternal, Message: fmt.Sprintf("Internal error: %v", r)},
				}
				b, _ := json.Marshal(errResp)
				session.enqueue(b)
			}
		}()
		resp := s.dispatchRequest(req)
		b, _ := json.Marshal(resp)
		session.enqueue(b)
	}()
}

func (s *mcpServer) handleNotification(req jsonRPCRequest) {
	// MCP notifications: "notifications/initialized", "notifications/cancelled", etc.
	// Notifications do not return responses.
}

func (s *mcpServer) dispatchRequest(req jsonRPCRequest) jsonRPCResponse {
	switch req.Method {
	case "initialize":
		var p mcpInitializeParams
		if len(req.Params) > 0 {
			_ = json.Unmarshal(req.Params, &p)
		}
		ver := Version
		if ver == "" {
			ver = "0.1.0"
		}
		res := mcpInitializeResult{
			ProtocolVersion: mcpProtocolVersion,
			Capabilities: map[string]any{
				"tools": map[string]any{
					"listChanged": false,
				},
			},
			Instructions: "Sandman peer-to-peer data and compute fabric daemon. Manage repos, commits, files, pipelines, jobs, and cluster nodes.",
		}
		res.ServerInfo.Name = "sandman"
		res.ServerInfo.Version = ver
		return jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: res}

	case "ping":
		return jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: map[string]any{}}

	case "tools/list":
		return jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"tools": s.tools,
			},
		}

	case "tools/call":
		var callParams mcpCallToolParams
		if err := json.Unmarshal(req.Params, &callParams); err != nil {
			return jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   &jsonRPCError{Code: mcpErrCodeInvalidParams, Message: "invalid tools/call params: " + err.Error()},
			}
		}
		handler, ok := s.handlers[callParams.Name]
		if !ok {
			return jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   &jsonRPCError{Code: mcpErrCodeInvalidParams, Message: fmt.Sprintf("unknown tool: %q", callParams.Name)},
			}
		}
		if callParams.Arguments == nil {
			callParams.Arguments = make(map[string]any)
		}
		res, err := handler(callParams.Arguments)
		if err != nil {
			return jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: &mcpToolResult{
					Content: []mcpContent{
						{Type: "text", Text: err.Error()},
					},
					IsError: true,
				},
			}
		}
		return jsonRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: res}

	default:
		return jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &jsonRPCError{Code: mcpErrCodeMethodNotFound, Message: fmt.Sprintf("Method not found: %s", req.Method)},
		}
	}
}

func (d *daemon) mcpSSEH(w http.ResponseWriter, r *http.Request) {
	d.getMCP().handleSSE(w, r)
}

func (d *daemon) mcpMessagesH(w http.ResponseWriter, r *http.Request) {
	d.getMCP().handleMessages(w, r)
}

// ---- Tool Registration and Helpers ----

func jsonResult(v any) (*mcpToolResult, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return &mcpToolResult{
		Content: []mcpContent{
			{Type: "text", Text: string(b)},
		},
	}, nil
}

func getString(args map[string]any, key string) (string, bool) {
	if v, ok := args[key]; ok && v != nil {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}

func getBool(args map[string]any, key string) bool {
	if v, ok := args[key]; ok && v != nil {
		switch b := v.(type) {
		case bool:
			return b
		case string:
			return b == "true" || b == "1"
		}
	}
	return false
}

func getInt(args map[string]any, key string, def int) int {
	if v, ok := args[key]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case string:
			if i, err := strconv.Atoi(n); err == nil {
				return i
			}
		}
	}
	return def
}

func getStringSlice(args map[string]any, key string) []string {
	if v, ok := args[key]; ok && v != nil {
		if list, ok := v.([]any); ok {
			var out []string
			for _, item := range list {
				if s, ok := item.(string); ok {
					out = append(out, s)
				}
			}
			return out
		}
		if list, ok := v.([]string); ok {
			return list
		}
	}
	return nil
}

func (s *mcpServer) registerTool(tool mcpTool, handler mcpToolHandler) {
	s.tools = append(s.tools, tool)
	s.handlers[tool.Name] = handler
}

func (s *mcpServer) registerTools() {
	d := s.d

	// 1. repo_list
	s.registerTool(mcpTool{
		Name:        "repo_list",
		Description: "List all repositories in the sandman fabric.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		if d.store == nil {
			return jsonResult([]client.Repo{})
		}
		repos, err := d.store.ListRepos()
		if err != nil {
			return nil, err
		}
		return jsonResult(repos)
	})

	// 2. repo_inspect
	s.registerTool(mcpTool{
		Name:        "repo_inspect",
		Description: "Inspect a repository by name to see its metadata, branches, and status.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Name of the repository to inspect",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", name)
		}
		repo, err := d.store.InspectRepo(name)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("repo %q not found", name)
			}
			return nil, err
		}
		return jsonResult(repo)
	})

	// 3. repo_create
	s.registerTool(mcpTool{
		Name:        "repo_create",
		Description: "Create a new versioned repository in the sandman fabric.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Unique name for the repository",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("store not initialized")
		}
		if err := d.store.CreateRepo(name); err != nil {
			return nil, err
		}
		return jsonResult(map[string]any{"name": name, "ok": true})
	})

	// 4. repo_delete
	s.registerTool(mcpTool{
		Name:        "repo_delete",
		Description: "Delete a repository. Fails if the repository is referenced by pipelines unless force is true.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Name of the repository to delete",
				},
				"force": map[string]any{
					"type":        "boolean",
					"description": "Force deletion even if referenced by pipelines",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", name)
		}
		force := getBool(args, "force")
		if err := d.store.DeleteRepo(name, force); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("repo %q not found", name)
			}
			return nil, err
		}
		return jsonResult(map[string]any{"name": name, "deleted": true})
	})

	// 5. branch_list
	s.registerTool(mcpTool{
		Name:        "branch_list",
		Description: "List all branches and their head commit IDs in a repository.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Name of the repository",
				},
			},
			"required": []string{"repo"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", repo)
		}
		branches, err := d.store.BranchRefs(repo)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("repo %q not found", repo)
			}
			return nil, err
		}
		return jsonResult(branches)
	})

	// 6. branch_inspect
	s.registerTool(mcpTool{
		Name:        "branch_inspect",
		Description: "Inspect a branch in a repository to view its current head commit ID.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Name of the repository",
				},
				"branch": map[string]any{
					"type":        "string",
					"description": "Name of the branch",
				},
			},
			"required": []string{"repo", "branch"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		branch, ok := getString(args, "branch")
		if !ok || branch == "" {
			return nil, fmt.Errorf("invalid arguments: branch name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", repo)
		}
		head, err := d.store.BranchHead(repo, branch)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("branch %q in repo %q not found", branch, repo)
			}
			return nil, err
		}
		return jsonResult(client.Branch{Repo: repo, Branch: branch, Head: head})
	})

	// 7. branch_create
	s.registerTool(mcpTool{
		Name:        "branch_create",
		Description: "Create or retarget a branch to point at an existing commit. This triggers pipelines watching the branch.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Name of the repository",
				},
				"branch": map[string]any{
					"type":        "string",
					"description": "Name of the branch to create or retarget",
				},
				"head": map[string]any{
					"type":        "string",
					"description": "Commit ID to point the branch at",
				},
			},
			"required": []string{"repo", "branch", "head"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		branch, ok := getString(args, "branch")
		if !ok || branch == "" {
			return nil, fmt.Errorf("invalid arguments: branch name is required")
		}
		head, ok := getString(args, "head")
		if !ok || head == "" {
			return nil, fmt.Errorf("invalid arguments: head commit ID is required")
		}
		if err := d.createBranch(repo, branch, head); err != nil {
			return nil, err
		}
		return jsonResult(map[string]any{"ok": true, "repo": repo, "branch": branch, "head": head})
	})

	// 8. branch_delete
	s.registerTool(mcpTool{
		Name:        "branch_delete",
		Description: "Delete a branch ref from a repository (default branch cannot be deleted).",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Name of the repository",
				},
				"branch": map[string]any{
					"type":        "string",
					"description": "Name of the branch to delete",
				},
			},
			"required": []string{"repo", "branch"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		branch, ok := getString(args, "branch")
		if !ok || branch == "" {
			return nil, fmt.Errorf("invalid arguments: branch name is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", repo)
		}
		if err := d.store.DeleteBranch(repo, branch); err != nil {
			return nil, err
		}
		return jsonResult(map[string]any{"ok": true, "repo": repo, "branch": branch})
	})

	// 9. commit_start
	s.registerTool(mcpTool{
		Name:        "commit_start",
		Description: "Open a new commit on a repository branch for writing files.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Repository name",
				},
				"branch": map[string]any{
					"type":        "string",
					"description": "Branch name (default: master)",
				},
				"description": map[string]any{
					"type":        "string",
					"description": "Optional description or commit message",
				},
			},
			"required": []string{"repo"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		branch, _ := getString(args, "branch")
		desc, _ := getString(args, "description")
		if d.store == nil {
			return nil, fmt.Errorf("repo %q not found", repo)
		}
		cm, err := d.store.StartCommit(repo, branch, desc)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("repo %q not found", repo)
			}
			return nil, err
		}
		return jsonResult(cm)
	})

	// 10. commit_finish
	s.registerTool(mcpTool{
		Name:        "commit_finish",
		Description: "Close and finalize an open commit, publishing its changes and triggering downstream pipelines.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Open commit ID to finalize",
				},
				"description": map[string]any{
					"type":        "string",
					"description": "Optional commit message description",
				},
				"empty": map[string]any{
					"type":        "boolean",
					"description": "Allow finishing the commit even if no files were changed",
				},
			},
			"required": []string{"commit_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		desc, _ := getString(args, "description")
		empty := getBool(args, "empty")
		if d.store == nil {
			return nil, fmt.Errorf("commit %q not found", commitID)
		}
		cm, err := d.store.FinishCommit(commitID, desc, empty)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("commit %q not found", commitID)
			}
			return nil, err
		}
		d.triggerForCommit(cm)
		return jsonResult(cm)
	})

	// 11. commit_inspect
	s.registerTool(mcpTool{
		Name:        "commit_inspect",
		Description: "Inspect a commit by its ID or repo@branch reference, showing metadata, parent, provenance, and subvenants.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Commit ID or repo@branch reference to inspect",
				},
			},
			"required": []string{"commit_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("commit %q not found", commitID)
		}
		cm, err := d.store.InspectCommit(commitID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("commit %q not found", commitID)
			}
			return nil, err
		}
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
		return jsonResult(cm)
	})

	// 12. commit_head
	s.registerTool(mcpTool{
		Name:        "commit_head",
		Description: "Get the HEAD commit for a repository and branch.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"repo": map[string]any{
					"type":        "string",
					"description": "Repository name",
				},
				"branch": map[string]any{
					"type":        "string",
					"description": "Branch name (default: master)",
				},
			},
			"required": []string{"repo"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		repo, ok := getString(args, "repo")
		if !ok || repo == "" {
			return nil, fmt.Errorf("invalid arguments: repository name is required")
		}
		branch, _ := getString(args, "branch")
		if branch == "" {
			branch = "master"
		}
		if d.store == nil {
			return nil, fmt.Errorf("commit not found for %s@%s", repo, branch)
		}
		cm, err := d.store.HeadCommitRec(repo, branch)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("commit not found for %s@%s", repo, branch)
			}
			return nil, err
		}
		return jsonResult(cm)
	})

	// 13. commit_delete
	s.registerTool(mcpTool{
		Name:        "commit_delete",
		Description: "Delete a commit and all downstream artifacts derived from it across the DAG.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Commit ID to delete",
				},
			},
			"required": []string{"commit_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		settling, err := runBoundedTool(func() error { return d.deleteCommit(commitID) })
		if err != nil {
			return nil, err
		}
		res := map[string]any{"ok": true, "commit_id": commitID}
		if settling {
			res["settling"] = true
			res["note"] = "deletion applied; in-flight jobs are still settling"
		}
		return jsonResult(res)
	})

	// 14. file_get
	s.registerTool(mcpTool{
		Name:        "file_get",
		Description: "Read the contents of a file from a commit.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Commit ID containing the file",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Path of the file inside the commit",
				},
			},
			"required": []string{"commit_id", "path"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		path, ok := getString(args, "path")
		if !ok || path == "" {
			return nil, fmt.Errorf("invalid arguments: path is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("file %q not found in commit %q", path, commitID)
		}
		data, err := d.store.GetFile(commitID, path)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("file %q not found in commit %q", path, commitID)
			}
			return nil, err
		}
		if utf8.Valid(data) {
			return &mcpToolResult{
				Content: []mcpContent{
					{Type: "text", Text: string(data)},
				},
			}, nil
		}
		return jsonResult(map[string]any{
			"path":     path,
			"size":     len(data),
			"encoding": "base64",
			"content":  base64.StdEncoding.EncodeToString(data),
		})
	})

	// 15. file_put
	s.registerTool(mcpTool{
		Name:        "file_put",
		Description: "Write or update a file in an open commit.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Open commit ID",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Path where the file should be saved",
				},
				"content": map[string]any{
					"type":        "string",
					"description": "Text content of the file",
				},
				"overwrite": map[string]any{
					"type":        "boolean",
					"description": "Whether to overwrite existing content (default: true)",
				},
			},
			"required": []string{"commit_id", "path", "content"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		path, ok := getString(args, "path")
		if !ok || path == "" {
			return nil, fmt.Errorf("invalid arguments: path is required")
		}
		content, ok := getString(args, "content")
		if !ok {
			return nil, fmt.Errorf("invalid arguments: content is required")
		}
		overwrite := true
		if _, exists := args["overwrite"]; exists {
			overwrite = getBool(args, "overwrite")
		}
		if d.store == nil {
			return nil, fmt.Errorf("commit %q not found", commitID)
		}
		var err error
		if overwrite {
			err = d.store.OverwriteFile(commitID, path, []byte(content))
		} else {
			err = d.store.PutFile(commitID, path, []byte(content))
		}
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("commit %q not found", commitID)
			}
			return nil, err
		}
		return jsonResult(map[string]any{
			"ok":            true,
			"commit_id":     commitID,
			"path":          path,
			"bytes_written": len(content),
		})
	})

	// 16. file_delete
	s.registerTool(mcpTool{
		Name:        "file_delete",
		Description: "Delete a file from an open commit.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Open commit ID",
				},
				"path": map[string]any{
					"type":        "string",
					"description": "Path of the file to delete",
				},
			},
			"required": []string{"commit_id", "path"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		path, ok := getString(args, "path")
		if !ok || path == "" {
			return nil, fmt.Errorf("invalid arguments: path is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("commit %q not found", commitID)
		}
		if err := d.store.DeleteFile(commitID, path); err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("file %q not found in commit %q", path, commitID)
			}
			return nil, err
		}
		return jsonResult(map[string]any{"ok": true, "commit_id": commitID, "path": path})
	})

	// 17. file_list
	s.registerTool(mcpTool{
		Name:        "file_list",
		Description: "List files in a commit, optionally filtered by a prefix glob pattern.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"commit_id": map[string]any{
					"type":        "string",
					"description": "Commit ID to list files from",
				},
				"glob": map[string]any{
					"type":        "string",
					"description": "Optional prefix pattern ending with '*' (e.g. 'data/*')",
				},
			},
			"required": []string{"commit_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		commitID, ok := getString(args, "commit_id")
		if !ok || commitID == "" {
			return nil, fmt.Errorf("invalid arguments: commit_id is required")
		}
		if d.store == nil {
			return nil, fmt.Errorf("commit %q not found", commitID)
		}
		files, err := d.store.ListFiles(commitID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return nil, fmt.Errorf("commit %q not found", commitID)
			}
			return nil, err
		}
		glob, _ := getString(args, "glob")
		if glob != "" {
			prefix, star, ok := strings.Cut(glob, "*")
			if !ok || strings.Contains(star, "*") {
				return nil, fmt.Errorf("invalid arguments: unsupported listing glob %q (prefix patterns only)", glob)
			}
			var out []client.FileInfo
			for _, f := range files {
				if strings.HasPrefix(f.Path, prefix) {
					out = append(out, f)
				}
			}
			files = out
		}
		return jsonResult(files)
	})

	// 18. pipeline_list
	s.registerTool(mcpTool{
		Name:        "pipeline_list",
		Description: "List all pipelines registered on the fabric.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Optional filter by pipeline name",
				},
				"history": map[string]any{
					"type":        "integer",
					"description": "Optional number of past specification revisions to include",
				},
			},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, _ := getString(args, "name")
		var history *int
		if _, exists := args["history"]; exists {
			h := getInt(args, "history", 0)
			history = &h
		}
		pipes, err := d.listPipelinesFiltered(history, name, false)
		if err != nil {
			return nil, err
		}
		return jsonResult(pipes)
	})

	// 19. pipeline_inspect
	s.registerTool(mcpTool{
		Name:        "pipeline_inspect",
		Description: "Inspect a pipeline definition, state, inputs, transform image/cmd, and outputs.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Pipeline name to inspect",
				},
				"ancestry": map[string]any{
					"type":        "integer",
					"description": "Optional past revisions count",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline name is required")
		}
		ancestry := getInt(args, "ancestry", 0)
		info, err := d.inspectPipeline(name, ancestry)
		if err != nil {
			return nil, err
		}
		return jsonResult(info)
	})

	// 20. pipeline_create
	s.registerTool(mcpTool{
		Name:        "pipeline_create",
		Description: "Create or update a pipeline from a specification definition.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"spec": map[string]any{
					"type":        "object",
					"description": "Pipeline specification containing name, transform (image, cmd), input, and optional parameters",
				},
			},
			"required": []string{"spec"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		var p client.Pipeline
		switch v := args["spec"].(type) {
		case string:
			if err := json.Unmarshal([]byte(v), &p); err != nil {
				return nil, fmt.Errorf("invalid arguments: cannot parse pipeline spec: %w", err)
			}
		case map[string]any:
			b, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("invalid arguments: cannot marshal pipeline spec: %w", err)
			}
			if err := json.Unmarshal(b, &p); err != nil {
				return nil, fmt.Errorf("invalid arguments: cannot parse pipeline spec: %w", err)
			}
		default:
			return nil, fmt.Errorf("invalid arguments: 'spec' must be an object or JSON string")
		}
		if p.Name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline specification must include 'name'")
		}
		if err := d.createPipeline(p); err != nil {
			return nil, err
		}
		return jsonResult(map[string]any{"ok": true, "pipeline": p.Name})
	})

	// 21. pipeline_delete
	s.registerTool(mcpTool{
		Name:        "pipeline_delete",
		Description: "Delete a pipeline and optionally its output repository.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Pipeline name to delete",
				},
				"force": map[string]any{
					"type":        "boolean",
					"description": "Force deletion even if dependent pipelines exist",
				},
				"keep_repo": map[string]any{
					"type":        "boolean",
					"description": "Preserve the output repository after pipeline deletion",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline name is required")
		}
		force := getBool(args, "force")
		keepRepo := getBool(args, "keep_repo")
		settling, err := runBoundedTool(func() error { return d.deletePipeline(name, force, keepRepo) })
		if err != nil {
			return nil, err
		}
		res := map[string]any{"ok": true, "pipeline": name}
		if settling {
			res["settling"] = true
			res["note"] = "deletion applied; in-flight jobs are still settling"
		}
		return jsonResult(res)
	})

	// 22. pipeline_start
	s.registerTool(mcpTool{
		Name:        "pipeline_start",
		Description: "Start a stopped pipeline so it resumes processing incoming commits.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Pipeline name to start",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline name is required")
		}
		settling, err := runBoundedTool(func() error { return d.startPipeline(name) })
		if err != nil {
			return nil, err
		}
		res := map[string]any{"ok": true, "pipeline": name, "state": "started"}
		if settling {
			res["settling"] = true
			res["note"] = "start applied; the pipeline is still spinning up its first job"
		}
		return jsonResult(res)
	})

	// 23. pipeline_stop
	s.registerTool(mcpTool{
		Name:        "pipeline_stop",
		Description: "Stop an active pipeline to pause processing without deleting its definition or output.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Pipeline name to stop",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline name is required")
		}
		settling, err := runBoundedTool(func() error { return d.stopPipeline(name) })
		if err != nil {
			return nil, err
		}
		res := map[string]any{"ok": true, "pipeline": name, "state": "stopped"}
		if settling {
			res["settling"] = true
			res["note"] = "stop applied; in-flight jobs are still settling"
		}
		return jsonResult(res)
	})

	// 24. pipeline_run
	s.registerTool(mcpTool{
		Name:        "pipeline_run",
		Description: "Triggers a manual run of the specified pipeline. Returns a job handle immediately rather than blocking until pipeline completion, so that the call does not exceed JSON-RPC protocol transport timeouts. The returned job record includes the new job ID, which can be monitored by calling job_inspect or polled with job_wait.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"name": map[string]any{
					"type":        "string",
					"description": "Pipeline name to trigger a manual run for",
				},
				"provenance": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": "Optional list of commit IDs to use as inputs",
				},
				"job_id": map[string]any{
					"type":        "string",
					"description": "Optional previous job ID whose input commit pairings should be re-executed",
				},
			},
			"required": []string{"name"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		name, ok := getString(args, "name")
		if !ok || name == "" {
			return nil, fmt.Errorf("invalid arguments: pipeline name is required")
		}
		provenance := getStringSlice(args, "provenance")
		jobID, _ := getString(args, "job_id")
		job, err := d.runPipeline(name, provenance, jobID)
		if err != nil {
			return nil, err
		}
		return jsonResult(job)
	})

	// 25. job_list
	s.registerTool(mcpTool{
		Name:        "job_list",
		Description: "List jobs across the fabric with optional filtering by pipeline, state, or output commit.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"pipeline": map[string]any{
					"type":        "string",
					"description": "Filter jobs by pipeline name",
				},
				"state": map[string]any{
					"type":        "string",
					"description": "Filter jobs by state (running, queued, success, failure, crashed, killed)",
				},
				"output_commit": map[string]any{
					"type":        "string",
					"description": "Filter jobs by output commit ID",
				},
			},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		pipeline, _ := getString(args, "pipeline")
		state, _ := getString(args, "state")
		outputCommit, _ := getString(args, "output_commit")

		var states []string
		if state != "" {
			states = []string{state}
		}
		jobs, err := d.listJobsFiltered(pipeline, outputCommit, states, true, nil, nil)
		if err != nil {
			return nil, err
		}
		return jsonResult(jobs)
	})

	// 26. job_inspect
	s.registerTool(mcpTool{
		Name:        "job_inspect",
		Description: "Inspect a job by ID to view full execution details, state, datums, inputs, and outputs.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"job_id": map[string]any{
					"type":        "string",
					"description": "Job ID or output commit ID to inspect",
				},
			},
			"required": []string{"job_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		jobID, ok := getString(args, "job_id")
		if !ok || jobID == "" {
			return nil, fmt.Errorf("invalid arguments: job_id is required")
		}
		job, err := d.inspectJob(jobID)
		if err != nil {
			return nil, err
		}
		return jsonResult(job)
	})

	// 27. job_wait
	s.registerTool(mcpTool{
		Name:        "job_wait",
		Description: "Waits for a job to reach a terminal state (success, failure, killed, crashed), bounded by a timeout (timeout_seconds, default 10, maximum 30). Returns the settled job details if finished, or returns the current non-terminal job state (terminal=false) if the timeout expires without settling, allowing the caller to poll repeatedly without blocking the protocol past its transport timeouts.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"job_id": map[string]any{
					"type":        "string",
					"description": "Job ID to wait for",
				},
				"timeout_seconds": map[string]any{
					"type":        "integer",
					"description": "Maximum seconds to wait (default: 10, max: 30)",
				},
			},
			"required": []string{"job_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		jobID, ok := getString(args, "job_id")
		if !ok || jobID == "" {
			return nil, fmt.Errorf("invalid arguments: job_id is required")
		}
		timeoutSec := getInt(args, "timeout_seconds", 10)
		if timeoutSec <= 0 {
			timeoutSec = 10
		}
		if timeoutSec > 30 {
			timeoutSec = 30
		}

		initialJob, err := d.inspectJob(jobID)
		if err != nil {
			return nil, err
		}
		if initialJob.State != stateRunning && initialJob.State != stateQueued {
			return jsonResult(map[string]any{
				"terminal": true,
				"job":      initialJob,
			})
		}

		timeout := time.Duration(timeoutSec) * time.Second
		deadline := time.Now().Add(timeout)
		var terminal bool
		d.waitFor(deadline, func() bool {
			j, err := d.inspectJob(jobID)
			if err != nil {
				return false
			}
			terminal = j.State != stateRunning && j.State != stateQueued
			return terminal
		})

		latestJob, err := d.inspectJob(jobID)
		if err != nil {
			return nil, err
		}
		terminal = latestJob.State != stateRunning && latestJob.State != stateQueued
		return jsonResult(map[string]any{
			"terminal": terminal,
			"job":      latestJob,
		})
	})

	// 28. job_cancel
	s.registerTool(mcpTool{
		Name:        "job_cancel",
		Description: "Cancel a running or queued job, terminating its containers and settling its state as killed.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"job_id": map[string]any{
					"type":        "string",
					"description": "Job ID to cancel",
				},
			},
			"required": []string{"job_id"},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		jobID, ok := getString(args, "job_id")
		if !ok || jobID == "" {
			return nil, fmt.Errorf("invalid arguments: job_id is required")
		}
		settling, err := runBoundedTool(func() error { return d.cancelJob(jobID) })
		if err != nil {
			return nil, err
		}
		res := map[string]any{"ok": true, "job_id": jobID, "cancelled": true}
		if settling {
			res["settling"] = true
			res["note"] = "cancel applied; the job is still settling"
		}
		return jsonResult(res)
	})

	// 29. job_logs
	s.registerTool(mcpTool{
		Name:        "job_logs",
		Description: "Retrieve captured log lines from a job execution.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"job_id": map[string]any{
					"type":        "string",
					"description": "Job ID to read logs from",
				},
				"pipeline": map[string]any{
					"type":        "string",
					"description": "Pipeline name to filter logs from if job_id is not specified",
				},
				"since": map[string]any{
					"type":        "string",
					"description": "Duration string (e.g. '5m', '1h') to restrict logs to recent entries",
				},
			},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		jobID, _ := getString(args, "job_id")
		pipeline, _ := getString(args, "pipeline")
		sinceStr, _ := getString(args, "since")

		f := &logFilter{
			pipeline: pipeline,
			job:      jobID,
		}
		if sinceStr != "" {
			win, err := time.ParseDuration(sinceStr)
			if err != nil {
				return nil, fmt.Errorf("invalid arguments: invalid since %q: %v", sinceStr, err)
			}
			f.since = time.Now().UTC().Add(-win)
		}
		if f.job != "" {
			j, err := d.inspectJob(f.job)
			if err != nil {
				return nil, err
			}
			f.jobID = j.ID
		} else if f.pipeline != "" {
			if err := d.requirePipeline(f.pipeline); err != nil {
				return nil, err
			}
		}
		lines, err := d.collectLogs(f)
		if err != nil {
			return nil, err
		}
		return jsonResult(map[string]any{"lines": lines})
	})

	// 30. node_list
	s.registerTool(mcpTool{
		Name:        "node_list",
		Description: "List peer daemon nodes discovered in the peer-to-peer fabric mesh.",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		if d.reg == nil {
			return jsonResult([]any{})
		}
		d.reg.loadStatic()
		d.reg.prune()
		peers := d.reg.list()
		return jsonResult(peers)
	})

	// 31. worker_list
	s.registerTool(mcpTool{
		Name:        "worker_list",
		Description: "List registered execution host workers and their capabilities (labels, CPUs, memory, GPUs).",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		if d.hosts == nil {
			return jsonResult([]any{})
		}
		hosts := d.hosts.list()
		return jsonResult(hosts)
	})

	// 32. stats_get
	s.registerTool(mcpTool{
		Name:        "stats_get",
		Description: "Get current cluster and node resource utilization statistics (CPU, memory, active containers, GPU counts).",
		InputSchema: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
	}, func(args map[string]any) (*mcpToolResult, error) {
		jobs, pipes, hosts, hostsGPU, gpus, spouts := d.fleetCounts()
		curCpu := d.cpuBusy.Load()
		stats := map[string]any{
			"cpu_busy_percent":   float64(curCpu) / 1000.0,
			"hosts_total":        hosts,
			"hosts_with_gpus":    hostsGPU,
			"gpus_total":         gpus,
			"spouts_total":       spouts,
			"jobs_by_state":      jobs,
			"pipelines_by_state": pipes,
		}
		return jsonResult(stats)
	})
}
