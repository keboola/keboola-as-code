package log

import (
	"context"
	"testing"
)

func TestNewDebugLogger_All(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.Error(context.Background(), "error")

	expected := `
{"level":"debug","message":"debug"}
{"level":"info","message":"info"}
{"level":"warn","message":"warn"}
{"level":"error","message":"error"}
`
	AssertJSONMessages(t, expected, logger.AllMessages())
}

func TestNewDebugLogger_Debug(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.ErrorfCtx(context.Background(), "error")

	expected := `{"level":"debug","message":"debug"}`
	AssertJSONMessages(t, expected, logger.DebugMessages())
}

func TestNewDebugLogger_Info(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.ErrorfCtx(context.Background(), "error")

	expected := `{"level":"info","message":"info"}`
	AssertJSONMessages(t, expected, logger.InfoMessages())
}

func TestNewDebugLogger_Warn(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.ErrorfCtx(context.Background(), "error")

	expected := `{"level":"warn","message":"warn"}`
	AssertJSONMessages(t, expected, logger.WarnMessages())
}

func TestNewDebugLogger_WarnOrError(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.Error(context.Background(), "error")

	expected := `
{"level":"warn","message":"warn"}
{"level":"error","message":"error"}
`
	AssertJSONMessages(t, expected, logger.WarnAndErrorMessages())
}

func TestNewDebugLogger_Error(t *testing.T) {
	t.Parallel()
	logger := NewDebugLogger()
	logger.Debug(context.Background(), "debug")
	logger.Info(context.Background(), "info")
	logger.Warn(context.Background(), "warn")
	logger.ErrorfCtx(context.Background(), "error")

	expected := `{"level":"error","message":"error"}`
	AssertJSONMessages(t, expected, logger.ErrorMessages())
}
