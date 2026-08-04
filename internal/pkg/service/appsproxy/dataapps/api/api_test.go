package api

import (
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola/management"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpclient"
)

// authHeaderRecorder is a Sandboxes Service stub that records the auth headers of each request.
type authHeaderRecorder struct {
	*httptest.Server
	lock    sync.Mutex
	headers []http.Header
}

func (v *authHeaderRecorder) Requests() []http.Header {
	v.lock.Lock()
	defer v.lock.Unlock()
	return append([]http.Header(nil), v.headers...)
}

func startAuthHeaderRecorder(t *testing.T) *authHeaderRecorder {
	t.Helper()

	recorder := &authHeaderRecorder{}
	recorder.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		recorder.lock.Lock()
		recorder.headers = append(recorder.headers, req.Header.Clone())
		recorder.lock.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(recorder.Close)

	return recorder
}

// writeTokenFile writes content to a new file in a temporary directory and returns its path.
func writeTokenFile(t *testing.T, content string) string {
	t.Helper()

	path := filesystem.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	return path
}

// TestAPI_ServiceAccountAuth checks that the projected Kubernetes ServiceAccount token
// is sent in the X-Kubernetes-Authorization header, without the surrounding whitespace.
func TestAPI_ServiceAccountAuth(t *testing.T) {
	t.Parallel()

	srv := startAuthHeaderRecorder(t)
	tokenPath := writeTokenFile(t, "my-sa-token\n")
	api := New(httpclient.New(httpclient.WithoutForcedHTTP2()), srv.URL, management.NewKeboolaServiceAccountAuth(tokenPath))

	require.NoError(t, api.NotifyAppUsage("app-1", time.Now()).SendOrErr(t.Context()))

	requests := srv.Requests()
	require.Len(t, requests, 1)
	assert.Equal(t, "Bearer my-sa-token", requests[0].Get("X-Kubernetes-Authorization"))
	assert.Empty(t, requests[0].Get("X-KBC-ManageApiToken"))
}

// TestAPI_ServiceAccountAuth_Rotation checks that the token file is read per request,
// so a token rotated by the kubelet is used without a restart.
func TestAPI_ServiceAccountAuth_Rotation(t *testing.T) {
	t.Parallel()

	srv := startAuthHeaderRecorder(t)
	tokenPath := writeTokenFile(t, "old-token")
	api := New(httpclient.New(httpclient.WithoutForcedHTTP2()), srv.URL, management.NewKeboolaServiceAccountAuth(tokenPath))

	require.NoError(t, api.NotifyAppUsage("app-1", time.Now()).SendOrErr(t.Context()))

	// Rotate the token
	require.NoError(t, os.WriteFile(tokenPath, []byte("new-token"), 0o600))
	require.NoError(t, api.NotifyAppUsage("app-1", time.Now()).SendOrErr(t.Context()))

	requests := srv.Requests()
	require.Len(t, requests, 2)
	assert.Equal(t, "Bearer old-token", requests[0].Get("X-Kubernetes-Authorization"))
	assert.Equal(t, "Bearer new-token", requests[1].Get("X-Kubernetes-Authorization"))
}

// TestAPI_ServiceAccountAuth_Errors checks that an unusable token file fails the request,
// so the problem is reported as an error instead of an unauthorized request.
func TestAPI_ServiceAccountAuth_Errors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		tokenPath   func(t *testing.T) string
		expectedErr string
	}{
		{
			name:        "missing file",
			tokenPath:   func(t *testing.T) string { t.Helper(); return filesystem.Join(t.TempDir(), "missing") },
			expectedErr: "failed to read service account token file",
		},
		{
			name:        "empty file",
			tokenPath:   func(t *testing.T) string { t.Helper(); return writeTokenFile(t, "  \n") },
			expectedErr: "service account token file is empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := startAuthHeaderRecorder(t)
			api := New(httpclient.New(httpclient.WithoutForcedHTTP2()), srv.URL, management.NewKeboolaServiceAccountAuth(tc.tokenPath(t)))

			err := api.GetAppConfig("app-1", "").SendOrErr(t.Context())
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErr)

			// The request must not be sent without the credentials.
			assert.Empty(t, srv.Requests())
		})
	}
}
