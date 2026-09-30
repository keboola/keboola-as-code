package proxy

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestRegisterGracefulShutdown(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name            string
		drainTimeout    time.Duration
		requestDuration time.Duration
		expectCompleted bool
		maxShutdown     time.Duration
	}{
		{
			name:            "in-flight request completes",
			drainTimeout:    5 * time.Second,
			requestDuration: 300 * time.Millisecond,
			expectCompleted: true,
			maxShutdown:     3 * time.Second,
		},
		{
			name:            "drain is bounded by the timeout",
			drainTimeout:    100 * time.Millisecond,
			requestDuration: 2 * time.Second,
			expectCompleted: false,
			maxShutdown:     time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			started := make(chan struct{})
			release := make(chan struct{})
			t.Cleanup(func() { close(release) })

			srv := &http.Server{
				ReadHeaderTimeout: time.Second,
				Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					close(started)
					select {
					case <-time.After(tc.requestDuration):
					case <-release:
					}
					w.WriteHeader(http.StatusOK)
				}),
			}

			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)

			logger := log.NewDebugLogger()
			proc := servicectx.New(servicectx.WithLogger(logger), servicectx.WithoutSignals())
			proc.Add(func(shutdown servicectx.ShutdownFn) {
				shutdown(t.Context(), srv.Serve(ln))
			})
			registerGracefulShutdown(proc, logger, srv, tc.drainTimeout)

			resCh := make(chan error, 1)
			go func() {
				req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String(), nil)
				if err != nil {
					resCh <- err
					return
				}
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					resCh <- err
					return
				}
				resCh <- res.Body.Close()
			}()
			<-started

			shutdownStart := time.Now()
			proc.Shutdown(t.Context(), errors.New("test"))
			proc.WaitForShutdown()
			assert.Less(t, time.Since(shutdownStart), tc.maxShutdown)

			if !tc.expectCompleted {
				return
			}
			select {
			case err := <-resCh:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("in-flight request did not complete")
			}
		})
	}
}
