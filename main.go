package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const Version = "0.1.0"

// One static binary, busybox-style: every verb is a subcommand, the daemon
// is just another verb. Install sandman once, run `sandman daemon` (or the
// systemd unit) and the node joins the fleet on its own.
var (
	// addrFlag selects the control plane for the data-plane verbs
	// (planectl.go); it defaults to the local daemon port.
	addrFlag = flag.String("addr", defaultAddr(), "control-plane address (data-plane verbs)")
	// tokenFlag is the credential sent with the data-plane verbs (SB-154).
	tokenFlag = flag.String("token", "", "auth token for the control plane")
)

func defaultAddr() string {
	if a := os.Getenv("SANDBOX_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:" + strconv.Itoa(DefaultPort)
}

func main() {
	flag.Usage = usage
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	switch args[0] {
	case "daemon":
		cmdDaemon(args[1:])
	case "worker":
		cmdWorker(args[1:])
	case "run":
		cmdRun(args[1:])
	case "nodes":
		cmdNodes(args[1:])
	case "stats":
		cmdStats(args[1:])
	case "dashboard":
		cmdDashboard(args[1:])
	case "attach":
		cmdAttach(args[1:])
	case "detach":
		cmdDetach(args[1:])
	case "repo":
		cmdRepo(args[1:])
	case "commit":
		cmdCommit(args[1:])
	case "file":
		cmdFile(args[1:])
	case "pipeline":
		cmdPipeline(args[1:])
	case "job":
		cmdJob(args[1:])
	case "flush":
		cmdFlush(args[1:])
	case "version":
		fmt.Printf("sandman %s\n", Version)
	default:
		fmt.Fprintf(os.Stderr, "sandman: unknown verb %q\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `sandman — a naive peer-to-peer docker fabric

usage: sandman [flags] <verb> [flags]

global flags:
  -addr <host:port>   control-plane address (default $SANDBOX_ADDR or 127.0.0.1:4242)
  -token <token>      auth token for the control plane

verbs:
  daemon              run the node side (advertises via mDNS, serves jobs)
  worker              run an execution host (joins the control plane, executes placed work)
  run <node> -- <image> <cmd...>   stream a job to a node (ssh-like)
  nodes               list the fleet (mDNS browse + registry files)
  stats               poll every node, emit fleet state as JSONL
  dashboard           live TUI overview: nodes, containers, cpu/mem
  attach <name> <addr>    remember a static peer (for non-mDNS networks)
  detach <name>       forget a static peer
  repo                create/list/inspect/delete repositories
  commit              list/inspect commits
  file                put/get/list files (repo@branch:path)
  pipeline            create/list/inspect/delete pipelines (spec via -f)
  job                 list/inspect jobs
  flush commit <repo@branch>   wait for a commit's downstream jobs
  version             print version

flags:
  -state <dir>        state directory (default /var/lib/sandman)
`)
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state dir for name resolution")
	var env multiFlag
	fs.Var(&env, "e", "env K=V (repeatable)")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) < 4 || rest[1] != "--" {
		die("usage: sandman run [-e K=V] <node> -- <image> <cmd...>", 2)
	}
	clientRun(rest[0], *state, rest[2], []string(env), rest[3:])
}

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	fs.Parse(args)
	clientStats(*state)
}

func cmdNodes(args []string) {
	fs := flag.NewFlagSet("nodes", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	fs.Parse(args)
	clientNodes(*state)
}

func cmdAttach(args []string) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) != 2 {
		die("usage: sandman attach <name> <addr>", 2)
	}
	if err := addStatic(*state, rest[0], rest[1]); err != nil {
		die("attach: "+err.Error(), 1)
	}
	fmt.Printf("attached %s -> %s\n", rest[0], rest[1])
}

func cmdDetach(args []string) {
	fs := flag.NewFlagSet("detach", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) != 1 {
		die("usage: sandman detach <name>", 2)
	}
	if err := removeStatic(*state, rest[0]); err != nil {
		die("detach: "+err.Error(), 1)
	}
	fmt.Printf("detached %s\n", rest[0])
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "sandman"
	}
	return h
}

func sanitizeName(s string) string {
	return strings.ReplaceAll(s, ".", "-")
}
