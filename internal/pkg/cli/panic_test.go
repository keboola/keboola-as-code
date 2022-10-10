package cli

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestProcessPanic(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(fmt.Errorf("test"), logger, logFilePath)
	assert.Equal(t, 1, exitCode)
	logStr := logger.AllMessages()
	assert.Contains(t, logStr, "DEBUG  Unexpected panic: test")
	assert.Contains(t, logStr, "DEBUG  Trace:")
	assert.Contains(t, logStr, "To help us diagnose the problem you can send us a crash report.")
}
