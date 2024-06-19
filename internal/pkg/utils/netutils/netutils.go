package netutils

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/stretchr/testify/require"
)

func FreePortForTest(t *testing.T) int {
	t.Helper()

	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	require.NoError(t, err)

	for {
		listener, err := net.ListenTCP("tcp", addr)
		require.NoError(t, err)
		port := listener.Addr().(*net.TCPAddr).Port

		lockFile := filepath.Join(os.TempDir(), fmt.Sprintf(`keboola_go_test_port_lock_%d`, port)) //nolint:forbidigo
		lock := flock.New(lockFile)
		locked, err := lock.TryLock()
		require.NoError(t, err)

		_ = listener.Close()

		if locked {
			t.Cleanup(func() {
				require.NoError(t, lock.Unlock())
				require.NoError(t, os.Remove(lockFile)) //nolint:forbidigo
			})
			return port
		}
	}
}

func WaitForTCP(addr string, timeout time.Duration) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
