package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"sandman/internal/cli"
)

// Version is the baked build version. Releases set it at build time via
// -ldflags "-X main.Version=0.0.1"; dev builds fall back to this value.
// The daemon reports it on /api/v1/version (api.go); the CLI shows it
// via cli.PrintVersion (internal/cli).
var Version = "0.0.1"

// One static binary, busybox-style: every verb is a subcommand, the daemon
// is just another verb. Install sandman once, run `sandman daemon` (or the
// systemd unit) and the node joins the fleet on its own. The data-plane
// verbs (repo/commit/branch/file/job/datum/pipeline/flush/secret/tag/
// logs/transaction/check) are cobra commands over the client package
// (internal/cli); the fleet and runtime verbs parse their own flags and
// receive raw args (DisableFlagParsing).
var (
	// addrFlag selects the control plane for the data-plane verbs
	// (internal/cli); it defaults to the local daemon port.
	addrFlag = flag.String("addr", defaultAddr(), "control-plane address (data-plane verbs)")
	// versionFlag prints the binary and daemon versions and exits.
	versionFlag = flag.Bool("version", false, "print binary and daemon versions")
)

func defaultAddr() string {
	if a := os.Getenv("SANDMAN_ADDR"); a != "" {
		return a
	}
	return "127.0.0.1:" + strconv.Itoa(DefaultPort)
}

// newRootCmd assembles the whole CLI: the fleet/runtime verbs (raw flag
// passthrough) and the cobra data-plane subtree.
func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "sandman",
		Short:        "sandman — a naive peer-to-peer container fabric",
		SilenceUsage: true,
	}
	for _, f := range []struct {
		use string
		run func([]string)
	}{
		{"daemon", cmdDaemon},
		{"worker", cmdWorker},
		{"run", cmdRun},
		{"nodes", cmdNodes},
		{"stats", cmdStats},
		{"dashboard", cmdDashboard},
		{"attach", cmdAttach},
		{"detach", cmdDetach},
		{"update", cmdUpdate},
	} {
		c := &cobra.Command{
			Use:                f.use,
			DisableFlagParsing: true,
			Run:                func(_ *cobra.Command, args []string) { f.run(args) },
		}
		root.AddCommand(c)
	}
	root.AddCommand(cli.Commands()...)
	return root
}

func main() {
	flag.Usage = usage
	flag.Parse()
	cli.SetAddr(*addrFlag)
	cli.SetVersion(Version)
	if *versionFlag {
		cli.PrintVersion()
		os.Exit(0)
	}
	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(2)
	}
	root := newRootCmd()
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `sandman — a naive peer-to-peer container fabric

usage: sandman [flags] <verb> [flags]

global flags:
  -addr <host:port>   control-plane address (default $SANDMAN_ADDR or 127.0.0.1:4242)

verbs:
  daemon              run the node side (advertises via mDNS, serves jobs)
  worker              run an execution host (joins the control plane, executes placed work)
  run <node> -- <image> <cmd...>   stream a job to a node (ssh-like)
  nodes               list the fleet (mDNS browse + registry files)
  stats               poll every node, emit fleet state as JSONL
  dashboard           live TUI overview: nodes, containers, cpu/mem
  attach <name> <addr>    remember a static peer (for non-mDNS networks)
  detach <name>       forget a static peer
  update [--check]    check GitHub releases and install the latest build
  version / -version  print binary and daemon versions
  repo                create/list/inspect/delete repositories
  commit              start/finish/list/inspect/delete commits
  branch              create/list branches
  get                 fetch files from commits (repo@branch:path)
  file                put/get/list/copy/delete files (repo@branch:path)
  check               consistency check (fsck analog)
  pipeline            create/update/list/inspect/delete/start/stop/run/run-cron/extract/edit
  job                 list/inspect/delete/stop jobs
  datum               list/inspect/restart datums
  flush commit <repo@branch>   wait for a commit's downstream jobs
  secret              create/inspect/list/delete secrets
  tag                 put/get/list tags
  logs                pipeline/job logs (--follow streams)
  transaction         start/finish/delete/list/inspect/resume/stop

flags (per verb):
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
