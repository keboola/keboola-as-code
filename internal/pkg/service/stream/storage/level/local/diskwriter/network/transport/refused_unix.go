//go:build !windows

package transport

import (
	"syscall"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// isConnectionRefused returns true if the error means that nothing listens on the dialed address.
func isConnectionRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}
