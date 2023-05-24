package task

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestErrorType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", errorType(nil))
	assert.Equal(t, "other", errorType(errors.New("some error")))
	assert.Equal(t, "context_canceled", errorType(errors.Errorf(`some error: %w`, context.Canceled)))
	assert.Equal(t, "deadline_exceeded", errorType(errors.Errorf(`some error: %w`, context.DeadlineExceeded)))
	assert.Equal(t, "net", errorType(&net.DNSError{}))
	assert.Equal(t, "net_timeout", errorType(&net.DNSError{IsTimeout: true}))
	assert.Equal(t, "other", errorType(errors.New("some error")))
}
