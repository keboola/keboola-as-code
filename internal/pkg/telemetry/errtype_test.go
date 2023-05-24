package telemetry

import (
	"context"
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
	assert.Equal(t, "other", ErrorType(errors.New("some error")))
}
