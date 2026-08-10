package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"golang.org/x/term"
)

// dashboard is a thin renderer over the stats layer: poll the fleet, draw
// nodes and their containers with memory bars, refresh on a ticker.
// tview/tcell (the Go equivalent of htop's ncurses) owns terminal handling:
// alternate screen, raw mode, resize, colors, and key input — so layout is
// cell-based and adapts to any terminal size instead of hand-padded ANSI.
func cmdDashboard(args []string) {
	fs := flag.NewFlagSet("dashboard", flag.ExitOnError)
	state := fs.String("state", DefaultState, "state directory")
	refresh := fs.Duration("refresh", 2*time.Second, "refresh interval")
	fs.Parse(args)

	if !term.IsTerminal(int(os.Stdin.Fd())) {
		die("dashboard needs a terminal", 1)
	}

	app := tview.NewApplication()

	title := tview.NewTextView().SetDynamicColors(true).
		SetText("[::b]sandman[::] · fabric overview")
	info := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignRight)
	header := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(title, 0, 1, false).
		AddItem(info, 0, 1, false)

	table := tview.NewTable().
		SetSelectable(false, false).
		SetFixed(1, 0)
	table.SetBorder(true).SetTitle(" nodes & containers ")

	footer := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter).
		SetText("[::d]j/k scroll · q quit · ctrl-c quit[::]")

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(header, 1, 0, false).
		AddItem(table, 0, 1, true).
		AddItem(footer, 1, 0, false)
	app.SetRoot(root, true)

	// Keyboard scrolling: the table clips below the fold on short terminals
	// (mouse wheel scrolls natively); j/k/PgUp/PgDn/Home/End move the offset.
	scrollRow := 0
	scroll := func(delta int) {
		scrollRow += delta
		if scrollRow < 0 {
			scrollRow = 0
		}
		table.SetOffset(scrollRow, 0)
	}
	app.SetInputCapture(func(ev *tcell.EventKey) *tcell.EventKey {
		switch ev.Key() {
		case tcell.KeyCtrlC, tcell.KeyEscape:
			app.Stop()
		case tcell.KeyPgUp:
			scroll(-10)
		case tcell.KeyPgDn:
			scroll(10)
		case tcell.KeyHome:
			scroll(-1 << 30)
		case tcell.KeyEnd:
			scroll(1 << 30)
		case tcell.KeyRune:
			switch ev.Rune() {
			case 'q':
				app.Stop()
			case 'j':
				scroll(1)
			case 'k':
				scroll(-1)
			}
		}
		return ev
	})

	apply := func(stats []nodeStats, now time.Time) {
		total, unreach := 0, 0
		for _, ns := range stats {
			total += len(ns.Containers)
			if ns.Error != "" {
				unreach++
			}
		}
		info.SetText(fmt.Sprintf("[::d]nodes %d · containers %d · %s[::]", len(stats), total, now.Format("15:04:05")))
		rows := drawTable(table, stats)
		if scrollRow > rows-1 {
			scrollRow = max(0, rows-1)
			table.SetOffset(scrollRow, 0)
		}
		if unreach > 0 {
			footer.SetText(fmt.Sprintf("[red]%d node(s) unreachable[::] · [::d]j/k scroll · q quit · ctrl-c quit[::]", unreach))
		} else {
			footer.SetText("[::d]j/k scroll · q quit · ctrl-c quit[::]")
		}
	}

	// Prime the first frame, then refresh off the UI thread: collection
	// blocks on docker stats (~seconds), so it never runs on the event loop.
	apply(collectStats(*state, 10*time.Second), time.Now())
	go func() {
		t := time.NewTicker(*refresh)
		defer t.Stop()
		for range t.C {
			stats := collectStats(*state, 10*time.Second)
			app.QueueUpdateDraw(func() { apply(stats, time.Now()) })
		}
	}()

	// SIGTERM (systemd stop, hub stop) must let tview restore the terminal.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)
	go func() {
		<-sig
		app.Stop()
	}()

	if err := app.Run(); err != nil {
		die("dashboard: "+err.Error(), 1)
	}
}

func drawTable(t *tview.Table, stats []nodeStats) int {
	t.Clear()
	headers := []string{"CONTAINER", "IMAGE", "CPU", "MEM"}
	for i, h := range headers {
		t.SetCell(0, i, tview.NewTableCell(h).SetTextColor(tcell.ColorGray))
	}
	if len(stats) == 0 {
		t.SetCell(1, 0, tview.NewTableCell("no nodes in the fleet yet — start `sandman daemon` on any host").SetTextColor(tcell.ColorGray))
		return 2
	}
	row := 1
	for _, ns := range stats {
		if ns.Error != "" {
			t.SetCell(row, 0, tview.NewTableCell("✗ "+ns.Node).SetTextColor(tcell.ColorRed).SetAttributes(tcell.AttrBold))
			t.SetCell(row, 1, tview.NewTableCell("unreachable: "+ns.Error).SetTextColor(tcell.ColorGray))
			row++
			continue
		}
		t.SetCell(row, 0, tview.NewTableCell(ns.Node).SetTextColor(tcell.ColorAqua).SetAttributes(tcell.AttrBold))
		t.SetCell(row, 1, tview.NewTableCell(fmt.Sprintf("%s · %d running", ns.Addr, len(ns.Containers))).SetTextColor(tcell.ColorGray))
		// node-level utilization: host cpu (5s sample) and real memory
		t.SetCell(row, 2, pctCell(ns.HostCpuPerc))
		mem := ""
		if ns.HostMemT > 0 {
			ratio := float64(ns.HostMemU) / float64(ns.HostMemT)
			mem = fmt.Sprintf("%s %s / %s · %.1f%%", memBar(ratio, 14), humanBytes(ns.HostMemU), humanBytes(ns.HostMemT), ns.HostMemPerc)
		}
		t.SetCell(row, 3, tview.NewTableCell(mem).SetTextColor(utilColor(ns.HostMemPerc)))
		row++
		for _, c := range ns.Containers {
			// Our jobs are named sandman-<jobid>; trim the prefix and leave
			// them full-brightness, dimming containers the fabric does not
			// own so the fleet's own work stands out.
			ours := strings.HasPrefix(c.Name, "sandman-")
			disp := c.Name
			if ours {
				disp = strings.TrimPrefix(disp, "sandman-")
			}
			color := tcell.ColorWhite
			if !ours {
				color = tcell.ColorGray
			}
			t.SetCell(row, 0, tview.NewTableCell(disp).SetTextColor(color))
			t.SetCell(row, 1, tview.NewTableCell(c.Image).SetTextColor(color))
			cpu := tview.NewTableCell("").SetTextColor(color)
			if c.CPU > 0 {
				cc := tcell.ColorGreen
				switch {
				case c.CPU > 80:
					cc = tcell.ColorRed
				case c.CPU > 40:
					cc = tcell.ColorYellow
				}
				cpu = tview.NewTableCell(fmt.Sprintf("%.1f%%", c.CPU)).SetTextColor(cc).SetAlign(tview.AlignRight)
			}
			t.SetCell(row, 2, cpu)
			mem := ""
			if c.MemLimit > 0 {
				ratio := float64(c.MemBytes) / float64(c.MemLimit)
				mem = fmt.Sprintf("%s %s / %s", memBar(ratio, 16), humanBytes(c.MemBytes), humanBytes(c.MemLimit))
			}
			t.SetCell(row, 3, tview.NewTableCell(mem).SetTextColor(color))
			row++
		}
	}
	return row
}

// pctCell formats a utilization percent, colored by load.
func pctCell(p float64) *tview.TableCell {
	if p <= 0 {
		return tview.NewTableCell("")
	}
	return tview.NewTableCell(fmt.Sprintf("%.1f%%", p)).SetTextColor(utilColor(p))
}

// utilColor: green idle, yellow busy, red saturated.
func utilColor(p float64) tcell.Color {
	switch {
	case p > 80:
		return tcell.ColorRed
	case p > 40:
		return tcell.ColorYellow
	default:
		return tcell.ColorGreen
	}
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
