package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDebugLogger(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Warn("test")
	assert.Equal(t, "WARN  test\n", logger.String())
}
