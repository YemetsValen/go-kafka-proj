// Package web embeds the Vite-built React UI into the server binary and
// exposes it as an http.Handler. If the embedded dist is empty (e.g. the
// frontend wasn't built), Handler returns nil so callers can opt out
// without breaking the rest of the routes.
package web

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var distFS embed.FS

// Handler returns an http.Handler that serves the embedded SPA. Static
// assets are served as-is; any unmatched path falls back to index.html so
// client-side routes (e.g. React Router) work after a hard reload.
//
// Returns nil when the embedded dist directory is empty (typical when running
// the binary outside of CI/Docker without first running `npm run build`).
func Handler() http.Handler {
	sub, err := fs.Sub(distFS, "dist")
	if err != nil {
		return nil
	}
	if _, err := fs.Stat(sub, "index.html"); err != nil {
		return nil
	}
	indexBytes, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		return nil
	}
	fileServer := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Try to serve the requested file directly; fall back to index.html
		// for unmatched non-asset paths so SPA routing keeps working.
		clean := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
		if clean == "" {
			clean = "index.html"
		}
		if _, err := fs.Stat(sub, clean); err == nil {
			fileServer.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexBytes)
	})
}
