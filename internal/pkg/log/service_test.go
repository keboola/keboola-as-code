package log

import (
	"context"
	"strings"
	"testing"
)

func TestServiceLogger_VerboseFalse(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	logger := NewServiceLogger(&out, false).WithComponent("component1")

	// Log messages
	logger.Debug(context.Background(), "Debug msg")
	logger.Info(context.Background(), "Info msg")
	logger.Warn(context.Background(), "Warn msg")
	logger.Error(context.Background(), "Error msg")

	// Log messages with a different component
	logger = logger.WithComponent("component2")
	logger.Debug(context.Background(), "Debug msg")
	logger.Info(context.Background(), "Info msg")
	logger.Warn(context.Background(), "Warn msg")
	logger.Error(context.Background(), "Error msg")

	// Assert
	expected := `
{"level":"info","message":"Info msg","component":"component1"}
{"level":"warn","message":"Warn msg","component":"component1"}
{"level":"error","message":"Error msg","component":"component1"}
{"level":"info","message":"Info msg","component":"component1.component2"}
{"level":"warn","message":"Warn msg","component":"component1.component2"}
{"level":"error","message":"Error msg","component":"component1.component2"}
`
	AssertJSONMessages(t, expected, out.String())
}

func TestServiceLogger_VerboseTrue(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	logger := NewServiceLogger(&out, true).WithComponent("component1")

	// Log messages
	logger.Debug(context.Background(), "Debug msg")
	logger.Info(context.Background(), "Info msg")
	logger.Warn(context.Background(), "Warn msg")
	logger.Error(context.Background(), "Error msg")

	// Log messages with a different component
	logger = logger.WithComponent("component2")
	logger.Debug(context.Background(), "Debug msg")
	logger.Info(context.Background(), "Info msg")
	logger.Warn(context.Background(), "Warn msg")
	logger.Error(context.Background(), "Error msg")

	// Assert
	expected := `
{"level":"debug","message":"Debug msg","component":"component1"}
{"level":"info","message":"Info msg","component":"component1"}
{"level":"warn","message":"Warn msg","component":"component1"}
{"level":"error","message":"Error msg","component":"component1"}
{"level":"debug","message":"Debug msg","component":"component1.component2"}
{"level":"info","message":"Info msg","component":"component1.component2"}
{"level":"warn","message":"Warn msg","component":"component1.component2"}
{"level":"error","message":"Error msg","component":"component1.component2"}
`
	AssertJSONMessages(t, expected, out.String())
}
