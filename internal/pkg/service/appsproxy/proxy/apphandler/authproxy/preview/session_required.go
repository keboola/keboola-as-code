package preview

import (
	"net/http"
	"strings"
)

// SessionRequiredMessageType is posted to the parent window by the page a frame gets without a preview session.
const SessionRequiredMessageType = "app-preview-session-required"

// IsFrameDocumentLoad reports a document load into an iframe or frame.
// Sec-Fetch-* can be forged by a non-browser client, so it only picks a page to show, never grants access.
func IsFrameDocumentLoad(req *http.Request) bool {
	dest := req.Header.Get("Sec-Fetch-Dest")
	if dest != "iframe" && dest != "frame" {
		return false
	}
	return strings.Contains(req.Header.Get("Accept"), "text/html")
}
