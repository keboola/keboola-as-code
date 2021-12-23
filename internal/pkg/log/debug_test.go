package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDebugLogger_All(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	assert.Equal(t, "DEBUG  debug\nINFO  info\nWARN  warn\nERROR  error\n", logger.AllMsgs())
	assert.Empty(t, logger.AllMsgs())
}

func TestNewDebugLogger_Debug(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Errorf("error")
	assert.Equal(t, "DEBUG  debug\n", logger.DebugMsgs())
	assert.Empty(t, logger.AllMsgs())
	assert.Empty(t, logger.DebugMsgs())
}

func TestNewDebugLogger_Info(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Errorf("error")
	assert.Equal(t, "INFO  info\n", logger.InfoMsgs())
	assert.Empty(t, logger.AllMsgs())
	assert.Empty(t, logger.InfoMsgs())
}

func TestNewDebugLogger_Warn(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Errorf("error")
	assert.Equal(t, "WARN  warn\n", logger.WarnMsgs())
	assert.Empty(t, logger.AllMsgs())
	assert.Empty(t, logger.WarnMsgs())
}

func TestNewDebugLogger_WarnOrError(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")
	assert.Equal(t, "WARN  warn\nERROR  error\n", logger.WarnOrErrorMsgs())
	assert.Empty(t, logger.AllMsgs())
	assert.Empty(t, logger.WarnOrErrorMsgs())
}

func TestNewDebugLogger_Error(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Errorf("error")
	assert.Equal(t, "ERROR  error\n", logger.ErrorMsgs())
	assert.Empty(t, logger.AllMsgs())
	assert.Empty(t, logger.ErrorMsgs())
}
