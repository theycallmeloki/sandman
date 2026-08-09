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

	// alternate screen, clear scrollback so the first frame starts fresh,
	// hide the cursor
	fmt.Print("\x1b[?1049h\x1b[3J\x1b[2J\x1b[?25l")
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
	width := 80
	if w, _, err := term.GetSize(int(os.Stdout.Fd())); err == nil && w > 0 {
		width = w
	}

	// Repaint in place: home + clear-below. Frames are rewritten over
	// themselves instead of appended, so nothing stitches together and
	// shrinking frames don't leave ghosts.
	var b strings.Builder
	b.WriteString("\x1b[H\x1b[J")

	total, unreach := 0, 0
	for _, ns := range stats {
		total += len(ns.Containers)
		if ns.Error != "" {
			unreach++
		}
	}
	info := fmt.Sprintf("nodes %d · containers %d · %s", len(stats), total, now.Format("15:04:05"))
	b.WriteString("\x1b[1;36msandman\x1b[0m · fabric overview")
	if pad := width - len("sandman · fabric overview") - len(info) - 3; pad > 4 {
		b.WriteString(strings.Repeat(" ", pad) + "\x1b[2m" + info + "\x1b[0m")
	} else {
		b.WriteString("  " + info)
	}
	b.WriteString("\n" + strings.Repeat("─", width) + "\n")

	if len(stats) == 0 {
		b.WriteString("\n  no nodes in the fleet yet — start `sandman daemon` on any host\n")
	} else {
		b.WriteString(fmt.Sprintf("\n  \x1b[2m%-24s %-16s %8s  %s\x1b[0m\n", "CONTAINER", "IMAGE", "CPU", "MEM"))
		for i, ns := range stats {
			if i > 0 {
				b.WriteString("\n")
			}
			drawNode(&b, ns)
		}
	}
	if unreach > 0 {
		fmt.Fprintf(&b, "\n\x1b[31m%d node(s) unreachable\x1b[0m\n", unreach)
	}
	b.WriteString("\n\x1b[2mq quit · ctrl-c quit\x1b[0m\n")
	os.Stdout.WriteString(b.String())
}

func drawNode(b *strings.Builder, ns nodeStats) {
	if ns.Error != "" {
		fmt.Fprintf(b, "\x1b[1m%s\x1b[0m \x1b[2m%s\x1b[0m \x1b[31m✗ unreachable: %s\x1b[0m\n", ns.Node, ns.Addr, ns.Error)
		return
	}
	fmt.Fprintf(b, "\x1b[1m%s\x1b[0m ▸ %s · docker %s · %d running\n", ns.Node, ns.Addr, ns.Docker, len(ns.Containers))
	for _, c := range ns.Containers {
		drawContainer(b, c)
	}
}

func drawContainer(b *strings.Builder, c containerInfo) {
	// Our jobs are named sandman-<jobid>; trim the prefix for display and
	// leave them full-brightness, while dimming containers the fabric does
	// not own so the fleet's own work stands out.
	ours := strings.HasPrefix(c.Name, "sandman-")
	disp := c.Name
	if ours {
		disp = strings.TrimPrefix(disp, "sandman-")
	}

	cpu := ""
	if c.CPU > 0 {
		cpu = fmt.Sprintf("%7s", fmt.Sprintf("%.1f%%", c.CPU))
		switch {
		case c.CPU > 80:
			cpu = "\x1b[31m" + cpu + "\x1b[0m"
		case c.CPU > 40:
			cpu = "\x1b[33m" + cpu + "\x1b[0m"
		}
	}
	mem := ""
	if c.MemLimit > 0 {
		ratio := float64(c.MemBytes) / float64(c.MemLimit)
		mem = fmt.Sprintf("%s %s / %s", memBar(ratio, 16), humanBytes(c.MemBytes), humanBytes(c.MemLimit))
	}

	open, close := "", ""
	if !ours {
		open, close = "\x1b[2m", "\x1b[0m"
	}
	fmt.Fprintf(b, "  %s%-24s %-16s %s  %s%s\n", open, clip(disp, 22), clip(c.Image, 16), cpu, mem, close)
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
