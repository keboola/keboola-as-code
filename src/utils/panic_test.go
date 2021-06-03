package utils

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
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
	logger, writer, buffer := NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(NewUserErrorWithCode(123, "test"), logger, logFilePath)
	err := writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, 123, exitCode)
	assert.Contains(t, buffer.String(), "DEBUG  User error panic: test")
	assert.Contains(t, buffer.String(), "DEBUG  Trace:")
	assert.Contains(t, buffer.String(), "Details can be found in the log file \"/foo/bar.log\".")

}

func TestProcessPanicUnexpected(t *testing.T) {
	logger, writer, buffer := NewDebugLogger()
	logFilePath := "/foo/bar.log"
	exitCode := ProcessPanic(fmt.Errorf("test"), logger, logFilePath)
	err := writer.Flush()
	assert.NoError(t, err)
	assert.Equal(t, 1, exitCode)
	assert.Contains(t, buffer.String(), "DEBUG  Unexpected panic: test")
	assert.Contains(t, buffer.String(), "DEBUG  Trace:")
	assert.Contains(t, buffer.String(), "To help us diagnose the problem you can send us a crash report.")
}
