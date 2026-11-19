package main

// `sandman run --worker <name> -- <image> <cmd...>`: execute an arbitrary
// container on a registered execution host, ad hoc. This is the missing
// CLI path onto a worker's POST /exec, which a pipeline already uses for
// placed datums: a transform author can run `uv run script.py` inside the
// exact amd64 image and host that will run the pipeline, with live output
// and the container's exit code.
//
// It reuses the pipeline wire contract (execRequest/execResult), not a
// parallel one. An ad-hoc run has no job and no output commit, so those
// fields are sent empty — documented on adhocExecRequest — and it has zero
// input sides: the worker tolerates that and mounts nothing under
// /sandman/in.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"sandman/client"
)

// adhocExecRequest builds the request for one ad-hoc execution. What an
// ad-hoc run sends where a pipeline datum would differ:
//
//   - JobID is empty: there is no job record, and the worker never reads
//     the field when running (it exists for the control plane's accounting).
//   - Sides is an explicit empty list: no input is shipped, so no side env
//     var or /sandman/in mount is created. The worker's exec path already
//     accepts zero sides.
//   - Cname is a fresh, unique container name. The worker needs it to name
//     the container, and a streamed client that disconnects kills it.
//   - Stream asks for live NDJSON output; an older worker ignores it and
//     answers with the usual single JSON result, which the client accepts.
//
// Env carries the --env KEY=VALUE entries verbatim; that is the only
// environment a transform author needs to reproduce a pipeline run.
func adhocExecRequest(workerName, image string, env, argv []string) execRequest {
	return execRequest{
		JobID:   "", // no job record exists for an ad-hoc run
		Index:   0,
		Attempt: 0,
		Cname: "sandman-exec-" + envName(workerName) + "-" +
			strconv.FormatInt(time.Now().UnixNano(), 10),
		Image:  image,
		Cmd:    append([]string{}, argv...),
		Env:    append([]string{}, env...),
		Sides:  []execSide{},
		Stream: true,
	}
}

// resolveWorkerHost finds a named worker in the daemon's host registry.
// The error is actionable: it says whether the registry is empty or which
// names it does list.
func resolveWorkerHost(hosts []client.HostInfo, name, addr string) (client.HostInfo, error) {
	if len(hosts) == 0 {
		return client.HostInfo{}, fmt.Errorf("unknown worker %q — daemon at %s has no registered workers", name, addr)
	}
	names := make([]string, 0, len(hosts))
	for _, h := range hosts {
		if h.Name == name {
			return h, nil
		}
		names = append(names, h.Name)
	}
	return client.HostInfo{}, fmt.Errorf("unknown worker %q — daemon at %s lists: %s", name, addr, strings.Join(names, ", "))
}

// clientWorkerRun resolves a worker through the control plane's registry,
// executes the ad-hoc request there, and returns the container's exit
// code. It reports setup failures through errOut and never prints an env
// value whose name looks secret.
func clientWorkerRun(ctx context.Context, worker, addr, image string, env, argv []string, out, errOut io.Writer) int {
	hosts, err := client.New(addr).Hosts()
	if err != nil {
		fmt.Fprintf(errOut, "sandman: cannot read the host registry from %s: %v\n", addr, err)
		return 1
	}
	h, err := resolveWorkerHost(hosts, worker, addr)
	if err != nil {
		fmt.Fprintf(errOut, "sandman: %v\n", err)
		return 1
	}
	req := adhocExecRequest(h.Name, image, env, argv)
	fmt.Fprintf(errOut, "sandman: worker %s (%s): %s %s\n", h.Name, h.Addr, image, strings.Join(argv, " "))
	if len(req.Env) > 0 {
		fmt.Fprintf(errOut, "sandman: env %s\n", strings.Join(maskEnv(req.Env), " "))
	}
	res, err := execOnWorker(ctx, "http://"+h.Addr, req, out)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(errOut, "sandman: interrupted")
			return 130
		}
		fmt.Fprintf(errOut, "sandman: worker %q is not responding at %s: %v\n", h.Name, h.Addr, err)
		return 1
	}
	if res.Error != "" {
		fmt.Fprintf(errOut, "sandman: worker %s: %s\n", h.Name, res.Error)
		return 1
	}
	return res.PrimaryCode
}

// clientWorkerRunCmd is the CLI wrapper: it installs signal cancellation
// (Ctrl-C cancels the request; a streaming worker kills the container) and
// returns the exit code for os.Exit.
func clientWorkerRunCmd(worker, addr, image string, env, argv []string) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return clientWorkerRun(ctx, worker, addr, image, env, argv, os.Stdout, os.Stderr)
}

// execOnWorker posts one request to a worker's /exec and decodes either
// the streamed NDJSON reply (live output) or the older single JSON result.
// A single-result reply's Log is written to out so the command still
// behaves like a run; a streamed reply was already written live.
func execOnWorker(ctx context.Context, base string, req execRequest, out io.Writer) (execResult, error) {
	b, err := json.Marshal(req)
	if err != nil {
		return execResult{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/exec", bytes.NewReader(b))
	if err != nil {
		return execResult{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return execResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return execResult{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if strings.HasPrefix(resp.Header.Get("Content-Type"), execStreamType) {
		var result *execResult
		dec := json.NewDecoder(resp.Body)
		for {
			var ev execStreamEvent
			if err := dec.Decode(&ev); err != nil {
				if err == io.EOF {
					break
				}
				return execResult{}, err
			}
			switch ev.Type {
			case "out":
				if _, err := out.Write(ev.Data); err != nil {
					return execResult{}, err
				}
			case "result":
				result = ev.Result
			}
		}
		if result == nil {
			return execResult{}, fmt.Errorf("worker closed the stream without a result")
		}
		return *result, nil
	}
	var res execResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return execResult{}, err
	}
	if res.Log != "" {
		if _, err := io.WriteString(out, res.Log); err != nil {
			return execResult{}, err
		}
	}
	return res, nil
}

// maskEnv renders env entries for the client's own log line, replacing the
// value of a secret-looking name with ***. The real values still go to the
// container; only the operator-facing log is redacted.
func maskEnv(env []string) []string {
	masked := make([]string, len(env))
	for i, e := range env {
		k, _, ok := strings.Cut(e, "=")
		if ok && secretEnvName(k) {
			masked[i] = k + "=***"
			continue
		}
		masked[i] = e
	}
	return masked
}

// secretEnvName reports whether an environment variable name looks like it
// carries a credential: *KEY*, *TOKEN*, or *SECRET* (case-insensitive).
func secretEnvName(name string) bool {
	upper := strings.ToUpper(name)
	return strings.Contains(upper, "KEY") ||
		strings.Contains(upper, "TOKEN") ||
		strings.Contains(upper, "SECRET")
}
