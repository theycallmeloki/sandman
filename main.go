package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"sandman/internal/cli"
)

// rootAddr is the root command's persistent --addr: `sandman repo list
// --addr host:port` lands here (the global -addr, parsed by the std flag
// package, still works in leading position). PersistentPreRun installs
// it into the data-plane CLI before any subcommand runs.
var rootAddr string

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
	// auth flags attach HTTP Basic Auth to every control-plane request.
	// Env fallbacks are applied after parsing so a flag default never
	// echoes a credential in help output.
	authUserFlag     = flag.String("auth-user", "", "control-plane HTTP Basic Auth user (default $SANDMAN_AUTH_USER)")
	authPasswordFlag = flag.String("auth-password", "", "control-plane HTTP Basic Auth password (default $SANDMAN_AUTH_PASSWORD; prefer the env or -auth-password-file)")
	authPasswordFile = flag.String("auth-password-file", "", "read the control-plane HTTP Basic Auth password from a file (default $SANDMAN_AUTH_PASSWORD_FILE)")
	// versionFlag prints the binary and daemon versions and exits.
	versionFlag = flag.Bool("version", false, "print binary and daemon versions")
)

// rootAuthUser etc. mirror the global auth flags for the persistent
// after-verb position (`sandman repo list --auth-user X`).
var rootAuthUser, rootAuthPass, rootAuthPassFile string

// leadingAuthConfigured records that the leading-position auth flags (or
// a complete environment pair) already installed credentials. The
// after-verb resolution must not replace them unless it was given its
// own auth flag.
var leadingAuthConfigured bool

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
		Short:        "sandman — a naive peer-to-peer docker fabric",
		SilenceUsage: true,
	}
	// --addr also works after the verb (`sandman repo list --addr X`);
	// the leading-position -addr is consumed by the std flag package
	root.PersistentFlags().StringVar(&rootAddr, "addr", "", "control-plane address (overrides $SANDMAN_ADDR)")
	root.PersistentFlags().StringVar(&rootAuthUser, "auth-user", "", "control-plane HTTP Basic Auth user (overrides $SANDMAN_AUTH_USER)")
	root.PersistentFlags().StringVar(&rootAuthPass, "auth-password", "", "control-plane HTTP Basic Auth password (prefer $SANDMAN_AUTH_PASSWORD or --auth-password-file)")
	root.PersistentFlags().StringVar(&rootAuthPassFile, "auth-password-file", "", "read the control-plane HTTP Basic Auth password from a file")
	root.PersistentPreRunE = func(_ *cobra.Command, _ []string) error {
		if rootAddr != "" {
			cli.SetAddr(rootAddr)
		}
		switch {
		case rootAuthUser != "" || rootAuthPass != "" || rootAuthPassFile != "":
			// After-verb flags may complete an environment-only pair
			// (e.g. SANDMAN_AUTH_USER + --auth-password).
			auth, err := authFromEnv(rootAuthUser, rootAuthPass, rootAuthPassFile)
			if err != nil {
				return fmt.Errorf("auth: %w", err)
			}
			cli.SetAuth(auth.User, auth.Password)
		case !leadingAuthConfigured:
			// No leading flags: apply a complete environment pair. A
			// partial one is not fatal here — a DisableFlagParsing verb
			// (daemon) may complete it from its own flags, and the
			// daemon validates the final pair itself.
			if auth, err := authFromEnv("", "", ""); err == nil && auth.enabled() {
				cli.SetAuth(auth.User, auth.Password)
			}
		}
		return nil
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
	auth, err := authFromEnv(*authUserFlag, *authPasswordFlag, *authPasswordFile)
	if err != nil {
		// An environment-only partial pair may be completed by the
		// after-verb --auth-* flags; only explicit leading flags are
		// terminal here. PersistentPreRunE re-resolves the environment
		// when no leading credentials were installed.
		if *authUserFlag != "" || *authPasswordFlag != "" || *authPasswordFile != "" {
			die("auth: "+err.Error(), 2)
		}
	} else {
		cli.SetAuth(auth.User, auth.Password)
		leadingAuthConfigured = auth.enabled()
	}
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
	fmt.Fprintf(os.Stderr, `sandman — a naive peer-to-peer docker fabric

usage: sandman [flags] <verb> [flags]

global flags:
  -addr <host:port>   control-plane address (default $SANDMAN_ADDR or 127.0.0.1:4242)
  -auth-user <user>   HTTP Basic Auth user for the control plane (default $SANDMAN_AUTH_USER)
  -auth-password <pw> HTTP Basic Auth password (default $SANDMAN_AUTH_PASSWORD;
                      prefer the env or -auth-password-file to keep it out of argv)
  -auth-password-file <path>  read the password from a file (default $SANDMAN_AUTH_PASSWORD_FILE)

verbs:
  daemon              run the node side (advertises via mDNS, serves jobs)
  worker              run an execution host (joins the control plane, executes placed work)
  run <node> -- <image> <cmd...>   stream a job to a node (ssh-like)
  run --worker <name> [--env K=V] -- <image> <cmd...>
                      execute ad hoc on an execution host (registry name)
  nodes               list the fleet (mDNS browse + registry files)
  stats               poll every node, emit fleet state as JSONL
  dashboard           live TUI overview: nodes, containers, cpu/mem
  attach <name> <addr>    remember a static peer (for non-mDNS networks)
  detach <name>       forget a static peer
  update [--check]    check GitHub releases and install the latest build
  version / -version  print binary and daemon versions

  put                 upload files (cp-like: put <src>... <repo@branch:path>)
  patch               deliver a git checkout's edits as a delta to a git-input repo
  get                 fetch files (repo@branch:path), -o writes to a file/dir
  ls                  list repos, or files in a repo
  cat                 fetch files to stdout
  ps                  list jobs (alias for job list)
  status              one-glance view: daemon, hosts, pipelines, jobs
  dag [--format json|dot] [--pipeline name] [--json]
                      pipeline/repo graph, or the subgraph around a pipeline

  repo                create/list/inspect/delete repositories
  commit              start/finish/list/inspect/delete commits
  branch              create/list branches
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
  backup [dest]       snapshot the full control-plane state to a tar.gz
  reset --yes         destroy every repo and pipeline (state to zero)
  completion bash|zsh|fish    generate shell completion

flags (per verb):
  -state <dir>        state directory (default %s)
`, defaultState())
}

type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func cmdRun(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	state := fs.String("state", defaultState(), stateFlagHelp)
	worker := fs.String("worker", "", "run ad hoc on this worker (name from the daemon host registry)")
	addr := fs.String("addr", defaultAddr(), "control-plane address for --worker (default $SANDMAN_ADDR or 127.0.0.1:4242)")
	var env multiFlag
	fs.Var(&env, "e", "env K=V (repeatable)")
	fs.Var(&env, "env", "env K=V (repeatable)")
	fs.Parse(args)
	rest := fs.Args()
	if *worker != "" {
		if len(rest) < 2 || rest[0] == "" {
			die("usage: sandman run --worker <name> [--env K=V] -- <image> <cmd...>", 2)
		}
		for _, e := range env {
			k, _, ok := strings.Cut(e, "=")
			if !ok || k == "" {
				die(fmt.Sprintf("invalid env %q: want KEY=VALUE", e), 2)
			}
		}
		os.Exit(clientWorkerRunCmd(*worker, *addr, rest[0], env, rest[1:]))
	}
	if len(rest) < 4 || rest[1] != "--" {
		die("usage: sandman run [-e K=V] <node> -- <image> <cmd...>\n       sandman run --worker <name> [-e K=V] -- <image> <cmd...>", 2)
	}
	clientRun(rest[0], *state, rest[2], []string(env), rest[3:])
}

func cmdStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	state := fs.String("state", defaultState(), stateFlagHelp)
	fs.Parse(args)
	clientStats(*state)
}

func cmdNodes(args []string) {
	fs := flag.NewFlagSet("nodes", flag.ExitOnError)
	state := fs.String("state", defaultState(), stateFlagHelp)
	addr := fs.String("addr", defaultAddr(), "control-plane address (default $SANDMAN_ADDR or 127.0.0.1:4242)")
	local := fs.Bool("local", false, "browse mDNS and local registry files instead of asking the daemon")
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	fs.Parse(args)
	if !*local {
		hosts, err := cli.ClientForAddr(*addr).Hosts()
		if err == nil {
			if *asJSON {
				b, _ := json.MarshalIndent(hosts, "", "  ")
				fmt.Println(string(b))
				return
			}
			rows := make([][]string, 0, len(hosts))
			for _, h := range hosts {
				var gpus []string
				for _, g := range h.Gpus {
					s := fmt.Sprintf("%d %s", g.Index, g.Name)
					if g.Busy {
						s += " (busy)"
					}
					gpus = append(gpus, s)
				}
				rows = append(rows, []string{h.Name, h.Addr, strings.Join(h.Labels, ","), strings.Join(gpus, ", "), h.Seen})
			}
			cli.RenderTable([]string{"NAME", "ADDR", "LABELS", "GPUS", "SEEN"}, rows)
			if len(rows) == 0 {
				fmt.Println("no registered hosts")
				return
			}
			// ADDR is what the control plane dials to place work, not a job
			// endpoint: `run` speaks the job protocol, which the daemon
			// serves on :4242 — a worker's endpoint answers the placement
			// API only, so pointing `run` at it is rejected.
			fmt.Println("\nADDR is where the control plane places work. `sandman run` targets a daemon")
			fmt.Println("(or any host:port speaking the job protocol); work on a worker is placed by a pipeline.")
			return
		}
		// daemon unreachable: fall back to the local mDNS browse
		fmt.Fprintf(os.Stderr, "sandman: daemon not reachable at %s — falling back to local browse\n", *addr)
	}
	clientNodes(*state)
}

func cmdAttach(args []string) {
	fs := flag.NewFlagSet("attach", flag.ExitOnError)
	state := fs.String("state", defaultState(), stateFlagHelp)
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
	state := fs.String("state", defaultState(), stateFlagHelp)
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
