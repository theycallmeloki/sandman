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
	"sandman/internal/cli"
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
		SetText("[::b]sandman[::] [gray]fabric dashboard[::-]")
	info := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignRight)
	header := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(title, 0, 1, false).
		AddItem(info, 0, 1, false)

	onlineCard := metricCard(" online ")
	containersCard := metricCard(" containers ")
	cpuCard := metricCard(" host cpu ")
	memCard := metricCard(" host memory ")
	summary := tview.NewFlex().SetDirection(tview.FlexColumn).
		AddItem(onlineCard, 0, 1, false).
		AddItem(containersCard, 0, 1, false).
		AddItem(cpuCard, 0, 1, false).
		AddItem(memCard, 0, 1, false)

	table := tview.NewTable().
		SetSelectable(false, false).
		SetFixed(1, 0)
	table.SetBorder(true).SetTitle(" fleet ")

	footer := tview.NewTextView().SetDynamicColors(true).SetTextAlign(tview.AlignCenter).
		SetText("[::d]j/k scroll · q quit · ctrl-c quit[::]")

	root := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(header, 1, 0, false).
		AddItem(summary, 3, 0, false).
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
		total, unreach, online := 0, 0, 0
		var cpuTotal, memTotal float64
		for _, ns := range stats {
			total += len(ns.Containers)
			if ns.Error != "" {
				unreach++
				continue
			}
			online++
			cpuTotal += ns.HostCpuPerc
			memTotal += ns.HostMemPerc
		}
		cpuAvg, memAvg := 0.0, 0.0
		if online > 0 {
			cpuAvg = cpuTotal / float64(online)
			memAvg = memTotal / float64(online)
		}
		info.SetText(fmt.Sprintf("[::d]online %d/%d · containers %d · cpu %.1f%% · mem %.1f%% · %s[::]", online, len(stats), total, cpuAvg, memAvg, now.Format("15:04:05")))
		onlineCard.SetText(fmt.Sprintf("[::b]%d/%d[::-]\n[::d]%d unreachable[::-]", online, len(stats), unreach))
		containersCard.SetText(fmt.Sprintf("[::b]%d[::-]\n[::d]running containers[::-]", total))
		cpuCard.SetText(fmt.Sprintf("[%s::b]%.1f%%[::-]\n[::d]fleet average[::-]", colorTag(utilColor(cpuAvg)), cpuAvg))
		memCard.SetText(fmt.Sprintf("[%s::b]%.1f%%[::-]\n[::d]fleet average[::-]", colorTag(utilColor(memAvg)), memAvg))
		width, _, err := term.GetSize(int(os.Stdin.Fd()))
		if err != nil || width <= 0 {
			width = 100
		}
		if width < 96 {
			root.ResizeItem(summary, 0, 0)
		} else {
			root.ResizeItem(summary, 3, 0)
		}
		rows := drawTable(table, stats, width)
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

func metricCard(title string) *tview.TextView {
	v := tview.NewTextView().
		SetDynamicColors(true).
		SetTextAlign(tview.AlignCenter)
	v.SetBorder(true).SetTitle(title).SetBorderColor(tcell.ColorDarkCyan)
	return v
}

func drawTable(t *tview.Table, stats []nodeStats, width int) int {
	t.Clear()
	columns := dashboardColumns(width)
	t.SetTitle(fmt.Sprintf(" fleet · %s ", columns.label))
	headers := make([]string, 0, len(columns.columns))
	for _, col := range columns.columns {
		headers = append(headers, col.title)
	}
	for i, h := range headers {
		t.SetCell(0, i, cell(h, tcell.ColorGray, columns.columns[i].align, true))
	}
	if len(stats) == 0 {
		t.SetCell(1, 0, tview.NewTableCell("no nodes in the fleet yet — start `sandman daemon` on any host").SetTextColor(tcell.ColorGray).SetExpansion(1))
		return 2
	}
	row := 1
	for _, ns := range stats {
		if ns.Error != "" {
			values := dashboardRow{
				kind:   "node",
				name:   "✗ " + ns.Node,
				status: "unreachable: " + ns.Error,
				place:  ns.Addr,
			}
			writeDashboardRow(t, row, columns, values, tcell.ColorRed, true)
			row++
			continue
		}
		name := ns.Node
		role := ns.Role
		if role == "" {
			role = "daemon"
		}
		status := fmt.Sprintf("%s · %d running", role, len(ns.Containers))
		if ns.HostCpus > 0 {
			status = fmt.Sprintf("%s · %d cpu", status, ns.HostCpus)
		}
		mem := "n/a"
		if ns.HostMemT > 0 {
			ratio := float64(ns.HostMemU) / float64(ns.HostMemT)
			mem = fmt.Sprintf("%s %s / %s · %.1f%%", memBar(ratio, columns.barWidth), cli.HumanSize(ns.HostMemU), cli.HumanSize(ns.HostMemT), ns.HostMemPerc)
		}
		writeDashboardRow(t, row, columns, dashboardRow{
			kind:    "node",
			name:    name,
			status:  status,
			place:   ns.Addr,
			cpu:     pctText(ns.HostCpuPerc),
			mem:     mem,
			pids:    containerCount(len(ns.Containers)),
			docker:  dash(ns.Docker),
			version: dash(ns.Version),
		}, tcell.ColorAqua, true)
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
			cpu := ""
			cpuColor := color
			if c.CPU > 0 {
				cpu = fmt.Sprintf("%.1f%%", c.CPU)
				cpuColor = utilColor(c.CPU)
			}
			mem := ""
			if c.MemLimit > 0 {
				ratio := float64(c.MemBytes) / float64(c.MemLimit)
				mem = fmt.Sprintf("%s %s / %s · %.1f%%", memBar(ratio, columns.barWidth), cli.HumanSize(c.MemBytes), cli.HumanSize(c.MemLimit), c.MemPerc)
			}
			values := dashboardRow{
				kind:   containerKind(ours),
				name:   "  " + disp,
				status: compactStatus(c.Status),
				place:  c.Image,
				cpu:    cpu,
				mem:    mem,
				pids:   pidText(c.PIDs),
				docker: shortID(c.ID),
			}
			writeDashboardRow(t, row, columns, values, color, false)
			if cpu != "" {
				setDashboardCell(t, row, columns, "cpu", cpu, cpuColor, tview.AlignRight, false)
			}
			row++
		}
	}
	return row
}

type dashboardColumn struct {
	key   string
	title string
	align int
}

type dashboardLayout struct {
	label    string
	barWidth int
	columns  []dashboardColumn
}

type dashboardRow struct {
	kind, name, status, place, cpu, mem, pids, docker, version string
}

func dashboardColumns(width int) dashboardLayout {
	switch {
	case width >= 150:
		return dashboardLayout{"wide", 18, []dashboardColumn{
			{"kind", "TYPE", tview.AlignLeft},
			{"name", "NODE / CONTAINER", tview.AlignLeft},
			{"status", "STATUS", tview.AlignLeft},
			{"cpu", "CPU", tview.AlignRight},
			{"mem", "MEMORY", tview.AlignLeft},
			{"pids", "PIDS", tview.AlignRight},
			{"place", "ADDR / IMAGE", tview.AlignLeft},
			{"docker", "RUNTIME / ID", tview.AlignLeft},
			{"version", "VERSION", tview.AlignLeft},
		}}
	case width >= 112:
		return dashboardLayout{"standard", 14, []dashboardColumn{
			{"kind", "TYPE", tview.AlignLeft},
			{"name", "NODE / CONTAINER", tview.AlignLeft},
			{"status", "STATUS", tview.AlignLeft},
			{"cpu", "CPU", tview.AlignRight},
			{"mem", "MEMORY", tview.AlignLeft},
			{"pids", "PIDS", tview.AlignRight},
			{"place", "ADDR / IMAGE", tview.AlignLeft},
		}}
	default:
		return dashboardLayout{"compact", 8, []dashboardColumn{
			{"name", "NODE / CONTAINER", tview.AlignLeft},
			{"status", "STATUS", tview.AlignLeft},
			{"cpu", "CPU", tview.AlignRight},
			{"mem", "MEM", tview.AlignLeft},
		}}
	}
}

func writeDashboardRow(t *tview.Table, row int, layout dashboardLayout, values dashboardRow, color tcell.Color, bold bool) {
	for col, def := range layout.columns {
		value := dashboardValue(values, def.key)
		t.SetCell(row, col, cell(value, color, def.align, bold))
	}
}

func setDashboardCell(t *tview.Table, row int, layout dashboardLayout, key string, value string, color tcell.Color, align int, bold bool) {
	for col, def := range layout.columns {
		if def.key == key {
			t.SetCell(row, col, cell(value, color, align, bold))
			return
		}
	}
}

func dashboardValue(row dashboardRow, key string) string {
	switch key {
	case "kind":
		return row.kind
	case "name":
		return row.name
	case "status":
		return row.status
	case "place":
		return row.place
	case "cpu":
		return row.cpu
	case "mem":
		return row.mem
	case "pids":
		return row.pids
	case "docker":
		return row.docker
	case "version":
		return row.version
	default:
		return ""
	}
}

func cell(text string, color tcell.Color, align int, bold bool) *tview.TableCell {
	c := tview.NewTableCell(text).SetTextColor(color).SetAlign(align).SetExpansion(1)
	if bold {
		c.SetAttributes(tcell.AttrBold)
	}
	return c
}

func pctText(p float64) string {
	if p <= 0 {
		return ""
	}
	return fmt.Sprintf("%.1f%%", p)
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

func colorTag(color tcell.Color) string {
	switch color {
	case tcell.ColorRed:
		return "red"
	case tcell.ColorYellow:
		return "yellow"
	default:
		return "green"
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

func containerKind(ours bool) string {
	if ours {
		return "job"
	}
	return "ctr"
}

func compactStatus(status string) string {
	status = strings.TrimSpace(status)
	if status == "" {
		return ""
	}
	if i := strings.Index(status, " ("); i > 0 {
		return status[:i]
	}
	return status
}

func containerCount(n int) string {
	if n == 0 {
		return "0"
	}
	return fmt.Sprintf("%d ctr", n)
}

func pidText(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("%d", n)
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
