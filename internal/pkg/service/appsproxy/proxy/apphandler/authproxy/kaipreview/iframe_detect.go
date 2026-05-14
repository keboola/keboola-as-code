package kaipreview

import (
	"net/http"
	"strings"
)

// IsIframeDocumentLoad heuristically detects a top-level iframe document load by
// looking at the Sec-Fetch-Dest header and Accept header. Used as a routing
// signal (not a security gate) to decide when to serve the bootstrap shim on a
// dev-mode app with no valid session cookie.
//
// The signal is UX-only because Sec-Fetch-* is forgeable by non-browser clients.
// The bootstrap shim itself is harmless without the postMessage handshake from a
// trusted IDE origin.
func IsIframeDocumentLoad(r *http.Request) bool {
	dest := r.Header.Get("Sec-Fetch-Dest")
	if dest != "iframe" && dest != "frame" {
		return false
	}
	return strings.Contains(r.Header.Get("Accept"), "text/html")
}
