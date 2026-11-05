//go:build windows

package transport

import (
	"golang.org/x/sys/windows"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// isConnectionRefused returns true if the error means that nothing listens on the dialed address.
// Windows reports the Winsock error, which is not syscall.ECONNREFUSED.
func isConnectionRefused(err error) bool {
	return errors.Is(err, windows.WSAECONNREFUSED)
}
