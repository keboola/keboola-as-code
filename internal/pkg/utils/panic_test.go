package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestNewUserError(t *testing.T) {
	t.Parallel()
	err := NewUserError("test")
	assert.Equal(t, "test", err.Error())
	assert.Equal(t, 1, err.ExitCode)
}

func TestNewUserErrorWithCode(t *testing.T) {
	t.Parallel()
	err := NewUserErrorWithCode(123, "test")
	assert.Equal(t, "test", err.Error())
	assert.Equal(t, 123, err.ExitCode)
}

func TestProcessPanicUserError(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(NewUserErrorWithCode(123, "test"), logger, logFilePath)
	assert.Equal(t, 123, exitCode)
	logStr := logger.String()
	assert.Contains(t, logStr, "DEBUG  User error panic: test")
	assert.Contains(t, logStr, "DEBUG  Trace:")
	assert.Contains(t, logStr, "Details can be found in the log file \"/foo/bar.log\".")
}

func TestProcessPanicUnexpected(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(fmt.Errorf("test"), logger, logFilePath)
	assert.Equal(t, 1, exitCode)
	logStr := logger.String()
	assert.Contains(t, logStr, "DEBUG  Unexpected panic: test")
	assert.Contains(t, logStr, "DEBUG  Trace:")
	assert.Contains(t, logStr, "To help us diagnose the problem you can send us a crash report.")
}
