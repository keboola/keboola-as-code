package log

import (
	"bytes"
	stdLog "log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestApiLogger_VerboseFalse(t *testing.T) {
	t.Parallel()

	out := new(bytes.Buffer)
	stdLogger := stdLog.New(out, "[std-prefix]", 0)
	logger := NewAPILogger(stdLogger, "[old-prefix] ", false)

	// Log messages
	assert.Equal(t, "[old-prefix] ", logger.Prefix())
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Log messages with a different prefix
	logger = logger.WithPrefix("[new-prefix] ")
	assert.Equal(t, "[new-prefix] ", logger.Prefix())
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert
	expected := `
[std-prefix][old-prefix] INFO Info msg
[std-prefix][old-prefix] WARN Warn msg
[std-prefix][old-prefix] ERROR Error msg
[std-prefix][new-prefix] INFO Info msg
[std-prefix][new-prefix] WARN Warn msg
[std-prefix][new-prefix] ERROR Error msg
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), out.String())
}

func TestApiLogger_VerboseTrue(t *testing.T) {
	t.Parallel()

	out := new(bytes.Buffer)
	stdLogger := stdLog.New(out, "[std-prefix]", 0)
	logger := NewAPILogger(stdLogger, "[old-prefix] ", true)

	// Log messages
	assert.Equal(t, "[old-prefix] ", logger.Prefix())
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Log messages with a different prefix
	logger = logger.WithPrefix("[new-prefix] ")
	assert.Equal(t, "[new-prefix] ", logger.Prefix())
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert
	expected := `
[std-prefix][old-prefix] DEBUG Debug msg
[std-prefix][old-prefix] INFO Info msg
[std-prefix][old-prefix] WARN Warn msg
[std-prefix][old-prefix] ERROR Error msg
[std-prefix][new-prefix] DEBUG Debug msg
[std-prefix][new-prefix] INFO Info msg
[std-prefix][new-prefix] WARN Warn msg
[std-prefix][new-prefix] ERROR Error msg
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), out.String())
}
