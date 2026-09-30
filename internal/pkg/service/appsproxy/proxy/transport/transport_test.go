package transport

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type testDeps struct {
	telemetry telemetry.Telemetry
}

func (d testDeps) Telemetry() telemetry.Telemetry {
	return d.telemetry
}

func TestTransport_ResponseHeaderTimeout(t *testing.T) {
	t.Parallel()

	const headersDelay = 300 * time.Millisecond

	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(headersDelay)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(app.Close)

	cases := []struct {
		name          string
		timeout       time.Duration
		expectedError string
	}{
		{name: "headers within timeout", timeout: 5 * time.Second},
		{name: "headers after timeout", timeout: 50 * time.Millisecond, expectedError: "timeout awaiting response headers"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rt, err := New(testDeps{telemetry: telemetry.NewNop()}, tc.timeout)
			require.NoError(t, err)

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, app.URL, nil)
			require.NoError(t, err)

			res, err := rt.RoundTrip(req)
			if tc.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, res.StatusCode)
			require.NoError(t, res.Body.Close())
		})
	}
}
