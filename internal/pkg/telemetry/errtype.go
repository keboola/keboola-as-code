package telemetry

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	"net"
	"strconv"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// ErrorType detect error type for a metric.
func ErrorType(err error) string {
	var netErr net.Error
	var storageAPIErr *keboola.StorageError
	var encryptionAPIErr *keboola.EncryptionError
	var schedulerAPIErr *keboola.SchedulerError
	var queueAPIErr *keboola.QueueError
	var workspacesAPIErr *keboola.WorkspacesError
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
	case errors.As(err, &storageAPIErr):
		return "storage_api_" + storageAPIErr.ErrCode
	case errors.As(err, &encryptionAPIErr):
		return "encryption_api_" + strconv.Itoa(encryptionAPIErr.ErrCode)
	case errors.As(err, &schedulerAPIErr):
		return "scheduler_api_" + strconv.Itoa(schedulerAPIErr.ErrCode)
	case errors.As(err, &queueAPIErr):
		return "queue_api_" + strconv.Itoa(queueAPIErr.ErrCode)
	case errors.As(err, &workspacesAPIErr):
		return "workspaces_api"
	default:
		return "other"
	}
}
