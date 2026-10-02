package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"sandman/internal/store"
)

// setupTestDaemon creates an initialized daemon with durable state in a temp dir.
func setupTestDaemon(t *testing.T) (*daemon, string, func()) {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "jobs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "logs"), 0o755); err != nil {
		t.Fatal(err)
	}
	st := store.New(dir)
	_ = st.CreateRepo("spec")

	d := &daemon{
		reg:     newRegistry(dir, "test-node"),
		state:   dir,
		name:    "test-node",
		store:   st,
		hosts:   newHostRegistry(30 * time.Second),
		runner:  processRunner{},
		running: map[string]*runningJob{},
	}
	d.text = textBackend{nodeName: "test-node", handleNodes: d.handleNodes, handleStats: d.handleStats, handleRun: d.handleRun}
	d.mcp = newMCPServer(d)

	// Start on a real TCP port with the shared-port multiplexer
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()

	apiConns := make(chan net.Conn)
	apiSrv := &http.Server{
		Handler:           d.apiHandler(),
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	srvDone := make(chan struct{})
	go func() {
		_ = apiSrv.Serve(&chanListener{ch: apiConns, done: srvDone})
	}()

	acceptDone := make(chan struct{})
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go guard(func() { d.serveConn(c, apiConns) })
		}
	}()

	cleanup := func() {
		// Mirror the daemon's own shutdown (daemon.go): settle every job this
		// test spawned. These tests run a real process runner, and the daemon
		// tests sharing this package are timing-sensitive about jobs settling,
		// so a process left running here steals the CPU they measure.
		d.cancelAllRunningJobs()
		ln.Close()
		close(acceptDone)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = apiSrv.Shutdown(ctx)
	}

	return d, addr, cleanup
}

type sseEvent struct {
	Event string
	Data  string
}

type sseReader struct {
	resp   *http.Response
	reader *bufio.Reader
}

func connectSSE(ctx context.Context, t *testing.T, sseURL string) (*sseReader, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, "GET", sseURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s failed: %v", sseURL, err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		t.Fatalf("expected Content-Type text/event-stream, got %s", ct)
	}

	sr := &sseReader{
		resp:   resp,
		reader: bufio.NewReader(resp.Body),
	}

	// Read initial endpoint event
	ev, err := sr.readEvent(5 * time.Second)
	if err != nil {
		resp.Body.Close()
		t.Fatalf("read endpoint event: %v", err)
	}
	if ev.Event != "endpoint" {
		resp.Body.Close()
		t.Fatalf("expected event: endpoint, got %s", ev.Event)
	}

	return sr, ev.Data
}

func (sr *sseReader) readEvent(timeout time.Duration) (*sseEvent, error) {
	type readRes struct {
		ev  *sseEvent
		err error
	}
	ch := make(chan readRes, 1)

	go func() {
		ev := &sseEvent{}
		for {
			line, err := sr.reader.ReadString('\n')
			if err != nil {
				ch <- readRes{nil, err}
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				// Empty line delimits an SSE event
				if ev.Event != "" || ev.Data != "" {
					ch <- readRes{ev, nil}
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				// Comment line (keep-alive)
				continue
			}
			if strings.HasPrefix(line, "event:") {
				ev.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				ev.Data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
	}()

	select {
	case res := <-ch:
		return res.ev, res.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("readEvent timed out after %s", timeout)
	}
}

func postMessage(t *testing.T, targetURL string, payload any) (*http.Response, error) {
	t.Helper()
	var b []byte
	switch v := payload.(type) {
	case string:
		b = []byte(v)
	case []byte:
		b = v
	default:
		var err error
		b, err = json.Marshal(v)
		if err != nil {
			return nil, err
		}
	}
	req, err := http.NewRequest("POST", targetURL, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

func extractSessionID(msgURL string) string {
	u, err := url.Parse(msgURL)
	if err != nil {
		return ""
	}
	return u.Query().Get("sessionId")
}

// TestSharedPortRouting verifies that the daemon serves MCP SSE, standard REST,
// and the text protocol concurrently on the same port without disturbance.
func TestSharedPortRouting(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	// 1. MCP SSE GET
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgURL := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()
	if !strings.Contains(msgURL, "/mcp/messages?sessionId=") {
		t.Fatalf("unexpected message URL: %s", msgURL)
	}

	// 2. Standard REST route GET /api/v1/repos
	resp, err := http.Get(fmt.Sprintf("http://%s/api/v1/repos", addr))
	if err != nil {
		t.Fatalf("REST GET /api/v1/repos failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected REST 200, got %d", resp.StatusCode)
	}

	// 3. Text protocol NODES command
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("text protocol dial failed: %v", err)
	}
	defer conn.Close()
	w := bufio.NewWriter(conn)
	r := bufio.NewReader(conn)
	if err := writeLine(w, "HELLO", ProtoVersion); err != nil {
		t.Fatalf("text protocol HELLO write failed: %v", err)
	}
	okTok, err := readLine(r)
	if err != nil || len(okTok) == 0 || okTok[0] != "OK" {
		t.Fatalf("text protocol HELLO failed: %v, %v", okTok, err)
	}
	if err := writeLine(w, "NODES"); err != nil {
		t.Fatalf("text protocol write failed: %v", err)
	}
	head, err := readLine(r)
	if err != nil || len(head) == 0 || head[0] != "NODES" {
		t.Fatalf("text protocol response invalid: %v, %v", head, err)
	}
}

// TestMCPHandshake tests the complete MCP initialization handshake and ping.
func TestMCPHandshake(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	// Send initialize
	initReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "initialize",
		"params": map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"clientInfo": map[string]string{
				"name":    "test-client",
				"version": "1.0",
			},
		},
	}
	resp, err := postMessage(t, msgURL, initReq)
	if err != nil {
		t.Fatalf("initialize POST failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK {
		t.Fatalf("expected initialize status 202/200, got %d", resp.StatusCode)
	}

	// Read initialize response from SSE stream
	ev, err := sr.readEvent(5 * time.Second)
	if err != nil {
		t.Fatalf("read initialize response event: %v", err)
	}
	if ev.Event != "message" {
		t.Fatalf("expected event: message, got %s", ev.Event)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
		t.Fatalf("unmarshal initialize response: %v\ndata: %s", err, ev.Data)
	}
	if string(rpcResp.ID) != "1" {
		t.Fatalf("expected response id 1, got %s", string(rpcResp.ID))
	}
	if rpcResp.Error != nil {
		t.Fatalf("initialize returned error: %+v", rpcResp.Error)
	}

	resMap, ok := rpcResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("expected map result, got %T", rpcResp.Result)
	}
	if resMap["protocolVersion"] != "2024-11-05" {
		t.Fatalf("expected protocolVersion 2024-11-05, got %v", resMap["protocolVersion"])
	}

	// Send notifications/initialized
	notifyReq := map[string]any{
		"jsonrpc": "2.0",
		"method":  "notifications/initialized",
	}
	nResp, err := postMessage(t, msgURL, notifyReq)
	if err != nil {
		t.Fatalf("notifications/initialized failed: %v", err)
	}
	defer nResp.Body.Close()
	if nResp.StatusCode != http.StatusAccepted && nResp.StatusCode != http.StatusOK {
		t.Fatalf("expected notification status 202/200, got %d", nResp.StatusCode)
	}

	// Send ping
	pingReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "ping",
	}
	pResp, err := postMessage(t, msgURL, pingReq)
	if err != nil {
		t.Fatalf("ping failed: %v", err)
	}
	defer pResp.Body.Close()

	evPing, err := sr.readEvent(5 * time.Second)
	if err != nil {
		t.Fatalf("read ping response: %v", err)
	}
	var rpcPing jsonRPCResponse
	if err := json.Unmarshal([]byte(evPing.Data), &rpcPing); err != nil {
		t.Fatalf("unmarshal ping response: %v", err)
	}
	if string(rpcPing.ID) != "2" {
		t.Fatalf("expected ping response id 2, got %s", string(rpcPing.ID))
	}
}

// TestMCPToolsList verifies that all required tools are declared with valid JSON Schemas.
func TestMCPToolsList(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	listReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      10,
		"method":  "tools/list",
		"params":  map[string]any{},
	}
	resp, err := postMessage(t, msgURL, listReq)
	if err != nil {
		t.Fatalf("tools/list POST failed: %v", err)
	}
	defer resp.Body.Close()

	ev, err := sr.readEvent(5 * time.Second)
	if err != nil {
		t.Fatalf("read tools/list response: %v", err)
	}

	var rpcResp struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  struct {
			Tools []mcpTool `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
		t.Fatalf("unmarshal tools/list response: %v", err)
	}

	tools := rpcResp.Result.Tools
	if len(tools) < 25 {
		t.Fatalf("expected at least 25 tools, got %d", len(tools))
	}

	requiredTools := []string{
		"repo_list", "repo_inspect", "repo_create", "repo_delete",
		"branch_list", "branch_inspect", "branch_create", "branch_delete",
		"commit_start", "commit_finish", "commit_inspect", "commit_head", "commit_delete",
		"file_get", "file_put", "file_delete", "file_list",
		"pipeline_list", "pipeline_inspect", "pipeline_create", "pipeline_delete",
		"pipeline_start", "pipeline_stop", "pipeline_run",
		"job_list", "job_inspect", "job_wait", "job_cancel", "job_logs",
		"node_list", "worker_list", "stats_get",
	}

	toolMap := make(map[string]mcpTool)
	for _, tool := range tools {
		if tool.Name == "" {
			t.Errorf("tool has empty name")
		}
		if tool.Description == "" {
			t.Errorf("tool %q has empty description", tool.Name)
		}
		if tool.InputSchema == nil || tool.InputSchema["type"] != "object" {
			t.Errorf("tool %q has invalid inputSchema: %+v", tool.Name, tool.InputSchema)
		}
		toolMap[tool.Name] = tool
	}

	for _, req := range requiredTools {
		if _, ok := toolMap[req]; !ok {
			t.Errorf("missing required tool: %q", req)
		}
	}
}

// TestMCPToolsCall_RealResources exercises the tools against real storage and resources.
func TestMCPToolsCall_RealResources(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	callTool := func(reqID int, name string, args map[string]any) (*mcpToolResult, *jsonRPCError) {
		req := map[string]any{
			"jsonrpc": "2.0",
			"id":      reqID,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      name,
				"arguments": args,
			},
		}
		resp, err := postMessage(t, msgURL, req)
		if err != nil {
			t.Fatalf("callTool %s POST failed: %v", name, err)
		}
		defer resp.Body.Close()

		ev, err := sr.readEvent(5 * time.Second)
		if err != nil {
			t.Fatalf("callTool %s read event: %v", name, err)
		}

		var rpcResp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  *mcpToolResult  `json:"result"`
			Error   *jsonRPCError   `json:"error"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
			t.Fatalf("callTool %s unmarshal response: %v\ndata: %s", name, err, ev.Data)
		}
		return rpcResp.Result, rpcResp.Error
	}

	// 1. Create a repository
	res, rpcErr := callTool(101, "repo_create", map[string]any{"name": "mcp-repo"})
	if rpcErr != nil {
		t.Fatalf("repo_create rpcErr: %+v", rpcErr)
	}
	if res.IsError {
		t.Fatalf("repo_create failed: %s", res.Content[0].Text)
	}

	// 2. List repos
	res, rpcErr = callTool(102, "repo_list", map[string]any{})
	if rpcErr != nil || res.IsError {
		t.Fatalf("repo_list failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "mcp-repo") {
		t.Fatalf("repo_list output did not include mcp-repo: %s", res.Content[0].Text)
	}

	// 3. Inspect repo
	res, rpcErr = callTool(103, "repo_inspect", map[string]any{"name": "mcp-repo"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("repo_inspect failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "mcp-repo") {
		t.Fatalf("repo_inspect output did not contain mcp-repo: %s", res.Content[0].Text)
	}

	// 4. Start a commit
	res, rpcErr = callTool(104, "commit_start", map[string]any{
		"repo":        "mcp-repo",
		"branch":      "master",
		"description": "initial mcp commit",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("commit_start failed: %v %v", rpcErr, res)
	}
	var cmRec struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(res.Content[0].Text), &cmRec); err != nil || cmRec.ID == "" {
		t.Fatalf("could not extract commit ID: %s", res.Content[0].Text)
	}
	commitID := cmRec.ID

	// 5. Put a file
	res, rpcErr = callTool(105, "file_put", map[string]any{
		"commit_id": commitID,
		"path":      "data/greeting.txt",
		"content":   "Hello Sandman from MCP!",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("file_put failed: %v %v", rpcErr, res)
	}

	// 6. List files in commit
	res, rpcErr = callTool(106, "file_list", map[string]any{
		"commit_id": commitID,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("file_list failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "data/greeting.txt") {
		t.Fatalf("file_list did not include data/greeting.txt: %s", res.Content[0].Text)
	}

	// 7. Get file from commit
	res, rpcErr = callTool(107, "file_get", map[string]any{
		"commit_id": commitID,
		"path":      "data/greeting.txt",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("file_get failed: %v %v", rpcErr, res)
	}
	if res.Content[0].Text != "Hello Sandman from MCP!" {
		t.Fatalf("file_get returned unexpected text: %s", res.Content[0].Text)
	}

	// 8. Finish commit
	res, rpcErr = callTool(108, "commit_finish", map[string]any{
		"commit_id": commitID,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("commit_finish failed: %v %v", rpcErr, res)
	}

	// 9. Inspect commit
	res, rpcErr = callTool(109, "commit_inspect", map[string]any{
		"commit_id": commitID,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("commit_inspect failed: %v %v", rpcErr, res)
	}

	// 10. Check head commit
	res, rpcErr = callTool(110, "commit_head", map[string]any{
		"repo":   "mcp-repo",
		"branch": "master",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("commit_head failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, commitID) {
		t.Fatalf("commit_head did not match commit ID %s: %s", commitID, res.Content[0].Text)
	}

	// 11. Read cluster stats
	res, rpcErr = callTool(111, "stats_get", map[string]any{})
	if rpcErr != nil || res.IsError {
		t.Fatalf("stats_get failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "hosts_total") {
		t.Fatalf("stats_get did not contain hosts_total: %s", res.Content[0].Text)
	}

	// 12. Read node list
	res, rpcErr = callTool(112, "node_list", map[string]any{})
	if rpcErr != nil || res.IsError {
		t.Fatalf("node_list failed: %v %v", rpcErr, res)
	}
}

// TestMCPSessionDisconnect_Cleanup verifies session cleanup upon stream disconnect.
func TestMCPSessionDisconnect_Cleanup(t *testing.T) {
	d, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	sessionID := extractSessionID(msgPath)
	if sessionID == "" {
		t.Fatalf("could not extract session ID from %s", msgPath)
	}

	if d.getMCP().sessionCount() != 1 {
		t.Fatalf("expected 1 active session, got %d", d.getMCP().sessionCount())
	}

	// Disconnect client
	cancel()
	sr.resp.Body.Close()

	// Wait for cleanup
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && d.getMCP().sessionCount() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if d.getMCP().sessionCount() != 0 {
		t.Fatalf("expected 0 active sessions after disconnect, got %d", d.getMCP().sessionCount())
	}

	// POST to the disconnected session should return 404
	postURL := fmt.Sprintf("http://%s%s", addr, msgPath)
	resp, err := postMessage(t, postURL, map[string]any{"jsonrpc": "2.0", "id": 1, "method": "ping"})
	if err != nil {
		t.Fatalf("post after disconnect failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for expired session, got %d", resp.StatusCode)
	}
}

// TestMCPConcurrentSessions verifies that multiple sessions run concurrently without cross-talk.
func TestMCPConcurrentSessions(t *testing.T) {
	d, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	const count = 5
	var wg sync.WaitGroup
	wg.Add(count)

	for i := 0; i < count; i++ {
		go func(sessionIndex int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
			defer sr.resp.Body.Close()

			msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

			// Ping with unique ID
			reqID := 1000 + sessionIndex
			pingReq := map[string]any{
				"jsonrpc": "2.0",
				"id":      reqID,
				"method":  "ping",
			}
			resp, err := postMessage(t, msgURL, pingReq)
			if err != nil {
				t.Errorf("session %d ping POST failed: %v", sessionIndex, err)
				return
			}
			resp.Body.Close()

			ev, err := sr.readEvent(5 * time.Second)
			if err != nil {
				t.Errorf("session %d read ping response failed: %v", sessionIndex, err)
				return
			}

			var rpcResp jsonRPCResponse
			if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
				t.Errorf("session %d unmarshal ping response failed: %v", sessionIndex, err)
				return
			}
			if string(rpcResp.ID) != fmt.Sprintf("%d", reqID) {
				t.Errorf("session %d received response for id %s, want %d", sessionIndex, string(rpcResp.ID), reqID)
			}
		}(i)
	}

	wg.Wait()

	// Wait for cleanup after all goroutines finish
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && d.getMCP().sessionCount() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	if d.getMCP().sessionCount() != 0 {
		t.Fatalf("expected all sessions cleaned up, remaining: %d", d.getMCP().sessionCount())
	}
}

// TestMCPMalformedFrame verifies that malformed frames are rejected with JSON-RPC error
// without taking down the daemon or leaking resources.
func TestMCPMalformedFrame(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	// Send invalid JSON
	resp, err := postMessage(t, msgURL, "{invalid-json-content-here")
	if err != nil {
		t.Fatalf("POST malformed frame failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for malformed frame, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	var rpcErr jsonRPCResponse
	if err := json.Unmarshal(body, &rpcErr); err != nil {
		t.Fatalf("expected JSON-RPC error response, got %s", string(body))
	}
	if rpcErr.Error == nil || rpcErr.Error.Code != mcpErrCodeParseError {
		t.Fatalf("expected parse error code %d, got %+v", mcpErrCodeParseError, rpcErr.Error)
	}

	// Verify parse error is received on SSE stream as well
	evErr, err := sr.readEvent(5 * time.Second)
	if err != nil {
		t.Fatalf("read parse error from SSE failed: %v", err)
	}
	if !strings.Contains(evErr.Data, fmt.Sprintf("%d", mcpErrCodeParseError)) {
		t.Fatalf("expected parse error code in SSE event, got %s", evErr.Data)
	}

	// Verify next valid request succeeds immediately
	pResp, err := postMessage(t, msgURL, map[string]any{"jsonrpc": "2.0", "id": 99, "method": "ping"})
	if err != nil {
		t.Fatalf("valid ping after malformed failed: %v", err)
	}
	pResp.Body.Close()

	ev, err := sr.readEvent(5 * time.Second)
	if err != nil {
		t.Fatalf("read ping response failed: %v", err)
	}
	if !strings.Contains(ev.Data, `"id":99`) {
		t.Fatalf("unexpected ping response: %s", ev.Data)
	}
}

// TestMCPErrors_Actionable verifies distinguishable, actionable errors for unknown tool,
// bad arguments, unknown repo, and unknown job.
func TestMCPErrors_Actionable(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	callTool := func(reqID int, name string, args map[string]any) (*mcpToolResult, *jsonRPCError) {
		req := map[string]any{
			"jsonrpc": "2.0",
			"id":      reqID,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      name,
				"arguments": args,
			},
		}
		resp, err := postMessage(t, msgURL, req)
		if err != nil {
			t.Fatalf("callTool %s POST failed: %v", name, err)
		}
		defer resp.Body.Close()

		ev, err := sr.readEvent(5 * time.Second)
		if err != nil {
			t.Fatalf("callTool %s read event: %v", name, err)
		}

		var rpcResp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  *mcpToolResult  `json:"result"`
			Error   *jsonRPCError   `json:"error"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
			t.Fatalf("callTool %s unmarshal response: %v\ndata: %s", name, err, ev.Data)
		}
		return rpcResp.Result, rpcResp.Error
	}

	// 1. Unknown tool
	res, rpcErr := callTool(201, "unknown_tool_xyz_999", map[string]any{})
	if rpcErr == nil || !strings.Contains(rpcErr.Message, "unknown tool") {
		t.Fatalf("expected unknown tool error, got res=%+v, rpcErr=%+v", res, rpcErr)
	}

	// 2. Bad arguments (missing required parameter 'name')
	res, rpcErr = callTool(202, "repo_inspect", map[string]any{})
	if rpcErr != nil {
		t.Fatalf("unexpected RPC error for bad arguments: %+v", rpcErr)
	}
	if !res.IsError || !strings.Contains(res.Content[0].Text, "invalid arguments") {
		t.Fatalf("expected invalid arguments error, got %+v", res)
	}

	// 3. Unknown repo
	res, rpcErr = callTool(203, "repo_inspect", map[string]any{"name": "nonexistent_repo_404"})
	if rpcErr != nil {
		t.Fatalf("unexpected RPC error for unknown repo: %+v", rpcErr)
	}
	if !res.IsError || !strings.Contains(res.Content[0].Text, `repo "nonexistent_repo_404" not found`) {
		t.Fatalf("expected 'repo not found' error, got %+v", res)
	}

	// 4. Unknown job
	res, rpcErr = callTool(204, "job_inspect", map[string]any{"job_id": "nonexistent_job_404"})
	if rpcErr != nil {
		t.Fatalf("unexpected RPC error for unknown job: %+v", rpcErr)
	}
	if !res.IsError || !strings.Contains(res.Content[0].Text, `job "nonexistent_job_404" not found`) {
		t.Fatalf("expected 'job not found' error, got %+v", res)
	}
}

// TestMCPSSE_HeldOpen verifies that the SSE stream can remain open and active.
func TestMCPSSE_HeldOpen(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()

	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	// Keep stream open, perform multiple calls across a time window
	for i := 1; i <= 3; i++ {
		time.Sleep(100 * time.Millisecond)
		resp, err := postMessage(t, msgURL, map[string]any{
			"jsonrpc": "2.0",
			"id":      i,
			"method":  "ping",
		})
		if err != nil {
			t.Fatalf("ping %d failed: %v", i, err)
		}
		resp.Body.Close()

		ev, err := sr.readEvent(5 * time.Second)
		if err != nil {
			t.Fatalf("read ping %d failed: %v", i, err)
		}
		if ev.Event != "message" || !strings.Contains(ev.Data, fmt.Sprintf(`"id":%d`, i)) {
			t.Fatalf("unexpected response: %+v", ev)
		}
	}
}

// TestMCPPipelineAndJobTools exercises pipeline lifecycle and job tools.
func TestMCPPipelineAndJobTools(t *testing.T) {
	d, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	// Register a test host for worker_list
	d.hosts.register("test-worker-1", "10.0.0.1:4242", []string{"gpu"}, []GpuInfo{{Index: 0, Name: "A100", MemoryMiB: 40960}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()
	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	callTool := func(reqID int, name string, args map[string]any) (*mcpToolResult, *jsonRPCError) {
		req := map[string]any{
			"jsonrpc": "2.0",
			"id":      reqID,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      name,
				"arguments": args,
			},
		}
		resp, err := postMessage(t, msgURL, req)
		if err != nil {
			t.Fatalf("callTool %s POST failed: %v", name, err)
		}
		defer resp.Body.Close()

		ev, err := sr.readEvent(5 * time.Second)
		if err != nil {
			t.Fatalf("callTool %s read event: %v", name, err)
		}

		var rpcResp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  *mcpToolResult  `json:"result"`
			Error   *jsonRPCError   `json:"error"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
			t.Fatalf("callTool %s unmarshal response: %v\ndata: %s", name, err, ev.Data)
		}
		return rpcResp.Result, rpcResp.Error
	}

	// 1. Worker list
	res, rpcErr := callTool(301, "worker_list", map[string]any{})
	if rpcErr != nil || res.IsError {
		t.Fatalf("worker_list failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "test-worker-1") {
		t.Fatalf("worker_list did not contain test-worker-1: %s", res.Content[0].Text)
	}

	// 2. Create input repo and commit
	_, _ = callTool(302, "repo_create", map[string]any{"name": "pipe-in"})
	res, _ = callTool(303, "commit_start", map[string]any{"repo": "pipe-in", "branch": "master"})
	var cmRec struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(res.Content[0].Text), &cmRec)
	_, _ = callTool(304, "file_put", map[string]any{"commit_id": cmRec.ID, "path": "file.txt", "content": "data"})
	_, _ = callTool(305, "commit_finish", map[string]any{"commit_id": cmRec.ID})

	// 3. Create pipeline
	pipeSpec := map[string]any{
		"name": "my-pipe",
		"transform": map[string]any{
			"image": "alpine",
			"cmd":   []string{"cat"},
		},
		"input": map[string]any{
			"repo": "pipe-in",
			"glob": "/*",
		},
	}
	res, rpcErr = callTool(306, "pipeline_create", map[string]any{"spec": pipeSpec})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_create failed: %v %v", rpcErr, res)
	}

	// 4. List pipelines
	res, rpcErr = callTool(307, "pipeline_list", map[string]any{})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_list failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "my-pipe") {
		t.Fatalf("pipeline_list did not include my-pipe: %s", res.Content[0].Text)
	}

	// 5. Inspect pipeline
	res, rpcErr = callTool(308, "pipeline_inspect", map[string]any{"name": "my-pipe"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_inspect failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "my-pipe") {
		t.Fatalf("pipeline_inspect did not contain my-pipe: %s", res.Content[0].Text)
	}

	// 6. Stop pipeline
	res, rpcErr = callTool(309, "pipeline_stop", map[string]any{"name": "my-pipe"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_stop failed: %v %v", rpcErr, res)
	}

	// 7. Start pipeline
	res, rpcErr = callTool(310, "pipeline_start", map[string]any{"name": "my-pipe"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_start failed: %v %v", rpcErr, res)
	}

	// 8. Run pipeline manually
	res, rpcErr = callTool(311, "pipeline_run", map[string]any{"name": "my-pipe"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_run failed: %v %v", rpcErr, res)
	}
	var jobRec struct {
		ID       string `json:"id"`
		Pipeline string `json:"pipeline"`
		State    string `json:"state"`
	}
	if err := json.Unmarshal([]byte(res.Content[0].Text), &jobRec); err != nil || jobRec.ID == "" {
		t.Fatalf("pipeline_run did not return valid job: %s", res.Content[0].Text)
	}

	// 9. Inspect job
	res, rpcErr = callTool(312, "job_inspect", map[string]any{"job_id": jobRec.ID})
	if rpcErr != nil || res.IsError {
		t.Fatalf("job_inspect failed: %v %v", rpcErr, res)
	}

	// 10. List jobs
	res, rpcErr = callTool(313, "job_list", map[string]any{"pipeline": "my-pipe"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("job_list failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, jobRec.ID) {
		t.Fatalf("job_list did not contain job ID: %s", res.Content[0].Text)
	}

	// 11. Wait on job (bounded timeout)
	res, rpcErr = callTool(314, "job_wait", map[string]any{"job_id": jobRec.ID, "timeout_seconds": 2})
	if rpcErr != nil || res.IsError {
		t.Fatalf("job_wait failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, "terminal") {
		t.Fatalf("job_wait did not report terminal status: %s", res.Content[0].Text)
	}

	// 12. Cancel job
	res, rpcErr = callTool(315, "job_cancel", map[string]any{"job_id": jobRec.ID})
	if rpcErr != nil || res.IsError {
		t.Fatalf("job_cancel failed: %v %v", rpcErr, res)
	}

	// 13. Read job logs
	res, rpcErr = callTool(316, "job_logs", map[string]any{"job_id": jobRec.ID})
	if rpcErr != nil || res.IsError {
		t.Fatalf("job_logs failed: %v %v", rpcErr, res)
	}

	// 14. Delete pipeline
	res, rpcErr = callTool(317, "pipeline_delete", map[string]any{"name": "my-pipe", "force": true})
	if rpcErr != nil || res.IsError {
		t.Fatalf("pipeline_delete failed: %v %v", rpcErr, res)
	}
}

// TestMCPBranchAndFileTools exercises branch operations, file delete, and commit delete.
func TestMCPBranchAndFileTools(t *testing.T) {
	_, addr, cleanup := setupTestDaemon(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sr, msgPath := connectSSE(ctx, t, fmt.Sprintf("http://%s/mcp/sse", addr))
	defer sr.resp.Body.Close()
	msgURL := fmt.Sprintf("http://%s%s", addr, msgPath)

	callTool := func(reqID int, name string, args map[string]any) (*mcpToolResult, *jsonRPCError) {
		req := map[string]any{
			"jsonrpc": "2.0",
			"id":      reqID,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      name,
				"arguments": args,
			},
		}
		resp, err := postMessage(t, msgURL, req)
		if err != nil {
			t.Fatalf("callTool %s POST failed: %v", name, err)
		}
		defer resp.Body.Close()

		ev, err := sr.readEvent(5 * time.Second)
		if err != nil {
			t.Fatalf("callTool %s read event: %v", name, err)
		}

		var rpcResp struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  *mcpToolResult  `json:"result"`
			Error   *jsonRPCError   `json:"error"`
		}
		if err := json.Unmarshal([]byte(ev.Data), &rpcResp); err != nil {
			t.Fatalf("callTool %s unmarshal response: %v\ndata: %s", name, err, ev.Data)
		}
		return rpcResp.Result, rpcResp.Error
	}

	// Create repo and commit
	_, _ = callTool(401, "repo_create", map[string]any{"name": "bf-repo"})
	res, _ := callTool(402, "commit_start", map[string]any{"repo": "bf-repo", "branch": "master"})
	var cmRec struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(res.Content[0].Text), &cmRec)

	// File put and delete in open commit
	_, _ = callTool(403, "file_put", map[string]any{"commit_id": cmRec.ID, "path": "test.txt", "content": "temp"})
	res, rpcErr := callTool(404, "file_delete", map[string]any{"commit_id": cmRec.ID, "path": "test.txt"})
	if rpcErr != nil || res.IsError {
		t.Fatalf("file_delete failed: %v %v", rpcErr, res)
	}

	// Put permanent file and finish
	_, _ = callTool(405, "file_put", map[string]any{"commit_id": cmRec.ID, "path": "kept.txt", "content": "permanent"})
	_, _ = callTool(406, "commit_finish", map[string]any{"commit_id": cmRec.ID})

	// Create branch pointing to this commit
	res, rpcErr = callTool(407, "branch_create", map[string]any{
		"repo":   "bf-repo",
		"branch": "feature",
		"head":   cmRec.ID,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("branch_create failed: %v %v", rpcErr, res)
	}

	// Inspect branch
	res, rpcErr = callTool(408, "branch_inspect", map[string]any{
		"repo":   "bf-repo",
		"branch": "feature",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("branch_inspect failed: %v %v", rpcErr, res)
	}
	if !strings.Contains(res.Content[0].Text, cmRec.ID) {
		t.Fatalf("branch_inspect did not contain head commit: %s", res.Content[0].Text)
	}

	// Delete branch
	res, rpcErr = callTool(409, "branch_delete", map[string]any{
		"repo":   "bf-repo",
		"branch": "feature",
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("branch_delete failed: %v %v", rpcErr, res)
	}

	// Delete commit
	res, rpcErr = callTool(410, "commit_delete", map[string]any{
		"commit_id": cmRec.ID,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("commit_delete failed: %v %v", rpcErr, res)
	}

	// Delete repo
	res, rpcErr = callTool(411, "repo_delete", map[string]any{
		"name":  "bf-repo",
		"force": true,
	})
	if rpcErr != nil || res.IsError {
		t.Fatalf("repo_delete failed: %v %v", rpcErr, res)
	}
}
