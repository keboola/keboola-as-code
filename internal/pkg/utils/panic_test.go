package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewUserError(t *testing.T) {
	err := NewUserError("test")
	assert.Equal(t, "test", err.Error())
	assert.Equal(t, 1, err.ExitCode)
}

func TestNewUserErrorWithCode(t *testing.T) {
	err := NewUserErrorWithCode(123, "test")
	assert.Equal(t, "test", err.Error())
	assert.Equal(t, 123, err.ExitCode)
}

func TestProcessPanicUserError(t *testing.T) {
	logger, writer := NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(NewUserErrorWithCode(123, "test"), logger, logFilePath)
	assert.Equal(t, 123, exitCode)
	assert.Contains(t, writer.String(), "DEBUG  User error panic: test")
	assert.Contains(t, writer.String(), "DEBUG  Trace:")
	assert.Contains(t, writer.String(), "Details can be found in the log file \"/foo/bar.log\".")
}

func TestProcessPanicUnexpected(t *testing.T) {
	logger, writer := NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(fmt.Errorf("test"), logger, logFilePath)
	assert.Equal(t, 1, exitCode)
	assert.Contains(t, writer.String(), "DEBUG  Unexpected panic: test")
	assert.Contains(t, writer.String(), "DEBUG  Trace:")
	assert.Contains(t, writer.String(), "To help us diagnose the problem you can send us a crash report.")
}
