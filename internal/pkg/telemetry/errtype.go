package telemetry

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"net"
)

// ErrorType detect error type for a metric.
func ErrorType(err error) string {
	var netErr net.Error
	errors.As(err, &netErr)
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case netErr != nil && netErr.Timeout():
		return "net_timeout"
	case netErr != nil:
		return "net"
	default:
		return "other"
	}
}
