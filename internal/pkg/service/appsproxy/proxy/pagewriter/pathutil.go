package pagewriter

import "strings"

// IsStreamlitHealthCheck detects Streamlit health check endpoints.
//
// WORKAROUND: This is a temporary solution for AJDA-1935 to prevent broken HTML
// from being displayed in Streamlit's error modal when the app is starting or unavailable.
// Streamlit makes health check requests to these endpoints and renders the response
// in an error dialog, which doesn't properly handle HTML responses.
//
// Limitations:
//   - This approach is Streamlit-specific and does not scale to other frameworks
//   - Only covers /_stcore/health and /_stcore/host-config endpoints
//   - Other Streamlit internal endpoints (e.g., /_stcore/stream) are not covered
//
// Long-term solution: Modify Streamlit to properly handle HTML error responses
// or provide a query parameter for clients to request plain text format.
func IsStreamlitHealthCheck(path string) bool {
	return strings.HasSuffix(path, "/_stcore/health") || strings.HasSuffix(path, "/_stcore/host-config")
}
