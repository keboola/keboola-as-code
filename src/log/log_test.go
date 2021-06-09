package log

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"testing"
)

func TestNewLogger(t *testing.T) {
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, false)
	assert.NotNil(t, logger)
}

func TestFileCore(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := os.Create(filePath)
	assert.NoError(t, err)

	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, file, false)

	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert, all levels logged with the level prefix
	expected := "DEBUG\tDebug msg\nINFO\tInfo msg\nWARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expected, utils.GetFileContent(filePath))
}

func TestConsoleCoreVerboseFalse(t *testing.T) {
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, false)

	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Flush
	err := stdout.Flush()
	assert.NoError(t, err)
	err = stderr.Flush()
	assert.NoError(t, err)

	// Assert
	// info      -> stdout
	// warn, err -> stderr
	expectedOut := "Info msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, stdout.Buffer.String())
	assert.Equal(t, expectedErr, stderr.Buffer.String())
}

func TestConsoleCoreVerboseTrue(t *testing.T) {
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, true)
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Flush
	err := stdout.Flush()
	assert.NoError(t, err)
	err = stderr.Flush()
	assert.NoError(t, err)

	// Assert
	// debug (verbose), info -> stdout
	// warn, err             -> stderr
	expectedOut := "Debug msg\nInfo msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, stdout.Buffer.String())
	assert.Equal(t, expectedErr, stderr.Buffer.String())
}

func TestToInfoWriter(t *testing.T) {
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(stdout, stderr, nil, false)
	_, err := ToInfoWriter(logger).Write([]byte("test\n"))
	assert.NoError(t, err)

	// Flush
	err = stdout.Flush()
	assert.NoError(t, err)
	err = stderr.Flush()
	assert.NoError(t, err)

	// Assert, written to stdout
	assert.Equal(t, "test\n", stdout.Buffer.String())
	assert.Equal(t, "", stderr.Buffer.String())
}

func TestToWarnWriter(t *testing.T) {
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(stdout, stderr, nil, false)
	_, err := ToWarnWriter(logger).Write([]byte("test\n"))
	assert.NoError(t, err)

	// Flush
	err = stdout.Flush()
	assert.NoError(t, err)
	err = stderr.Flush()
	assert.NoError(t, err)

	// Assert, written to stderr
	assert.Equal(t, "", stdout.Buffer.String())
	assert.Equal(t, "test\n", stderr.Buffer.String())
}
