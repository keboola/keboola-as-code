package kaipreview

import (
	"net/http"
	"strings"
)

// WriteFrameAncestorsCSP sets a Content-Security-Policy header with a single
// frame-ancestors directive. Pass the allowed IDE origins; pass nil/empty to
// fall back to 'none' (deny all embedding).
func WriteFrameAncestorsCSP(w http.ResponseWriter, allowedOrigins []string) {
	if len(allowedOrigins) == 0 {
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		return
	}
	w.Header().Set("Content-Security-Policy", "frame-ancestors "+strings.Join(allowedOrigins, " "))
}
