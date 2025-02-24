package netutils

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func FreePortForTest(tb testing.TB) int {
	tb.Helper()

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(tb, err)

	for {
		listener, err := net.ListenTCP("tcp", addr)
		require.NoError(tb, err)
		port := listener.Addr().(*net.TCPAddr).Port

		lockFile := filepath.Join(os.TempDir(), fmt.Sprintf(`keboola_go_test_port_lock_%d`, port)) //nolint:forbidigo
		lock := flock.New(lockFile)
		locked, err := lock.TryLock()
		require.NoError(tb, err)

		_ = listener.Close()

		if locked {
			tb.Cleanup(func() {
				require.NoError(tb, lock.Unlock())
				require.NoError(tb, os.Remove(lockFile)) //nolint:forbidigo
			})
			return port
		}
	}
}

func WaitForTCP(addr string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, errors.New("TCP timeout"))
	defer cancel()

	var conn net.Conn
	for {
		select {
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
			return err
		default:
			conn, err = net.DialTimeout("tcp", addr, time.Second)
			if err == nil {
				_ = conn.Close()
				return nil
			}
		}
	}
}

func WaitForHTTP(url string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, errors.New("HTTP timeout"))
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			if err == nil {
				err = ctx.Err()
			}
			return err
		default:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				return nil
			}
		}
	}
}
