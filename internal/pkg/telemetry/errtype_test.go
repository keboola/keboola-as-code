package telemetry

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestErrorType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", ErrorType(nil))
	assert.Equal(t, "other", ErrorType(errors.New("some error")))
	assert.Equal(t, "context_canceled", ErrorType(errors.Errorf(`some error: %w`, context.Canceled)))
	assert.Equal(t, "deadline_exceeded", ErrorType(errors.Errorf(`some error: %w`, context.DeadlineExceeded)))
	assert.Equal(t, "net", ErrorType(&net.DNSError{}))
	assert.Equal(t, "net_timeout", ErrorType(&net.DNSError{IsTimeout: true}))
	assert.Equal(t, "storage_api_foo", ErrorType(&keboola.StorageError{ErrCode: "foo"}))
	assert.Equal(t, "encryption_api_123", ErrorType(&keboola.EncryptionError{ErrCode: 123}))
	assert.Equal(t, "scheduler_api_123", ErrorType(&keboola.SchedulerError{ErrCode: 123}))
	assert.Equal(t, "queue_api_123", ErrorType(&keboola.QueueError{ErrCode: 123}))
	assert.Equal(t, "workspaces_api", ErrorType(&keboola.WorkspacesError{}))
	assert.Equal(t, "other", ErrorType(errors.New("some error")))
}
