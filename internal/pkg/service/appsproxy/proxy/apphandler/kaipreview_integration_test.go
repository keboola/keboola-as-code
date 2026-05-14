package apphandler_test

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/kaipreview"
)

// TestApphandler_KaiPreview_FullFlow is a scaffolded end-to-end integration test
// for the kai-preview routing decisions wired into serveHTTPOrError.
//
// The full harness requires the complete proxy stack (Service scope, fake K8s client,
// test HTTP server) which lives in proxy_test.go. The test scenarios below should be
// migrated to proxy_test.go once the STA mock server is available alongside the test
// app server (Task 16 / T15 follow-up).
//
// Scenarios to implement:
//  1. Dev-mode app: GET /_proxy/kai-preview/bootstrap → 200 with shim HTML.
//  2. Dev-mode app: GET /_proxy/kai-preview/embed-token (OPTIONS preflight, allowed origin) → 204.
//  3. Dev-mode app: GET / with valid session cookie → upstream (no OAuth redirect).
//  4. Dev-mode app: GET / without cookie, Sec-Fetch-Dest=iframe, Accept=text/html → bootstrap shim (200).
//  5. Dev-mode app: GET / without cookie, Sec-Fetch-Dest=document → falls through to AuthRules.
//  6. Non-dev-mode app: GET /_proxy/kai-preview/bootstrap → 404 (dev-mode gate inside kaipreview handler).
//  7. Flip DevMode=false on live app: all /_proxy/kai-preview/* return 404; stale cookie → AuthRules.
//
// See proxy_test.go testCase harness and the "devmode" app fixture for the integration pattern.
func TestApphandler_KaiPreview_FullFlow(t *testing.T) {
	t.Parallel()
	t.Skip("Scaffolded — fill in alongside the full-flow test in T15/T16 once STA mock server is available. See proxy_test.go for the test harness pattern and the 'devmode' app fixture.")

	// Satisfy the import so the file compiles cleanly.
	_ = kaipreview.SessionCookieName
}
