package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"
)

// dashboard is a thin renderer over the stats layer: poll the fleet, draw
// nodes and their containers with memory bars, refresh on a ticker. Plain
// ANSI — alternate screen, block characters for bars, no TUI framework.
func cmdDashboard(args []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	refresh := fs.Duration("refresh", 2*time.Second, "refresh interval")
	fs.Parse(args)

	fd := int(os.Stdin.Fd())
	old, err := term.MakeRaw(fd)
	if err != nil {
		die("dashboard needs a terminal: "+err.Error(), 1)
	}
	defer term.Restore(fd, old)

	fmt.Print("\x1b[?1049h\x1b[?25l") // alternate screen, hide cursor
	defer fmt.Print("\x1b[?25h\x1b[?1049l")

	// Raw mode disables ISIG, so Ctrl-C arrives as byte 0x03; q quits.
	quit := make(chan struct{})
	go func() {
		buf := make([]byte, 16)
		for {
			n, err := os.Stdin.Read(buf)
			if err != nil {
				close(quit)
				return
			}
			for _, b := range buf[:n] {
				if b == 'q' || b == 0x03 {
					close(quit)
					return
				}
			}
		}
	}()
	// SIGTERM (systemd stop, hub stop) must restore the terminal too.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sig
		os.Exit(0) // defers run on exit()
	}()

	render := func() {
		drawDashboard(collectStats(*state, 10*time.Second), time.Now())
	}
	render()
	t := time.NewTicker(*refresh)
	defer t.Stop()
	for {
		select {
		case <-quit:
			return
		case <-t.C:
			render()
		}
	}
}

func drawDashboard(stats []nodeStats, now time.Time) {
	var b strings.Builder
	fmt.Fprintf(&b, "\x1b[2J\x1b[H") // clear + home
	total := 0
	for _, ns := range stats {
		total += len(ns.Containers)
	}
	fmt.Fprintf(&b, "\x1b[1msandman — fabric overview\x1b[0m   nodes %d · containers %d · %s\n\n",
		len(stats), total, now.Format("15:04:05"))

	if len(stats) == 0 {
		b.WriteString("no nodes in the fleet yet — start `sandman daemon` on any host\n")
	} else {
		for _, ns := range stats {
			if ns.Error != "" {
				fmt.Fprintf(&b, "\x1b[1m%-16s\x1b[0m \x1b[2m%s\x1b[0m \x1b[31munreachable: %s\x1b[0m\n",
					ns.Node, ns.Addr, ns.Error)
				continue
			}
			fmt.Fprintf(&b, "\x1b[1m%-16s\x1b[0m %s docker %s — %d container(s)\n",
				ns.Node, ns.Addr, ns.Docker, len(ns.Containers))
			for _, c := range ns.Containers {
				cpu := ""
				if c.CPU > 0 {
					cpu = fmt.Sprintf("%6.2f%%", c.CPU)
				}
				mem := ""
				if c.MemLimit > 0 {
					ratio := float64(c.MemBytes) / float64(c.MemLimit)
					mem = fmt.Sprintf("%s %s / %s", memBar(ratio, 18), humanBytes(c.MemBytes), humanBytes(c.MemLimit))
				}
				fmt.Fprintf(&b, "  %-24s %-16s %s %s\n", clip(c.Name, 22), clip(c.Image, 16), cpu, mem)
			}
		}
	}
	b.WriteString("\nq quit · ctrl-c quit\n")
	os.Stdout.WriteString(b.String())
}

// memBar renders a ratio as a block-character bar.
func memBar(ratio float64, width int) string {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	filled := int(ratio * float64(width))
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}

func humanBytes(n uint64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f := float64(n)
	i := 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%dB", n)
	}
	return fmt.Sprintf("%.1f%s", f, units[i])
}

func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
