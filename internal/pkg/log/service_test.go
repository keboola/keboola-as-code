package log

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestServiceLogger_VerboseFalse(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	logger := NewServiceLogger(&out, false).AddPrefix("[prefix1]")

	// Log messages
	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Log messages with a different prefix
	logger = logger.AddPrefix("[prefix2]")
	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Assert
	expected := `
[prefix1]INFO Info msg
[prefix1]WARN Warn msg
[prefix1]ERROR Error msg
[prefix1][prefix2]INFO Info msg
[prefix1][prefix2]WARN Warn msg
[prefix1][prefix2]ERROR Error msg
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), out.String())
}

func TestServiceLogger_VerboseTrue(t *testing.T) {
	t.Parallel()

	var out strings.Builder
	logger := NewServiceLogger(&out, true).AddPrefix("[prefix1]")

	// Log messages
	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Log messages with a different prefix
	logger = logger.AddPrefix("[prefix2]")
	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Assert
	expected := `
[prefix1]DEBUG Debug msg
[prefix1]INFO Info msg
[prefix1]WARN Warn msg
[prefix1]ERROR Error msg
[prefix1][prefix2]DEBUG Debug msg
[prefix1][prefix2]INFO Info msg
[prefix1][prefix2]WARN Warn msg
[prefix1][prefix2]ERROR Error msg
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), out.String())
}
