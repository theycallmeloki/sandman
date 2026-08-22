package main

// Embedded read-only web dashboard. The daemon serves it on the same
// port as the HTTP API: GET / returns index.html, /ui/* the static
// assets (Vue vendored, no build step, no external CDN), and everything
// under /api/v1/ is untouched — the dashboard is a pure GET consumer of
// the existing API. All writes stay in the CLI; the UI never issues a
// mutating request.

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

//go:embed web
var webUI embed.FS

// webIndexH serves the dashboard's index.html at GET /.
func (d *daemon) webIndexH(w http.ResponseWriter, r *http.Request) error {
	b, err := webUI.ReadFile("web/index.html")
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(b)
	return nil
}

// webAssetH serves a dashboard asset from the embedded FS under /ui/.
// Assets are revalidated rather than cached so a new daemon binary's
// dashboard replaces the old one immediately.
func (d *daemon) webAssetH(w http.ResponseWriter, r *http.Request) error {
	sub, err := fs.Sub(webUI, "web")
	if err != nil {
		return err
	}
	name := strings.TrimPrefix(r.URL.Path, "/ui/")
	f, err := sub.Open(name)
	if err != nil {
		// a missing asset is a 404, not the generic 400 hErr would
		// produce for a returned error
		writeErr(w, http.StatusNotFound, "no such asset: "+name)
		return nil
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	if st.IsDir() {
		return fmt.Errorf("no such asset: %s", name)
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		return fmt.Errorf("asset not seekable: %s", name)
	}
	ct := mime.TypeByExtension(path.Ext(name))
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, name, st.ModTime(), rs)
	return nil
}
