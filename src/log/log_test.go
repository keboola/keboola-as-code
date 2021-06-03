package log

import (
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"testing"
)

func TestNewLogger(t *testing.T) {
	writerOut, _ := utils.NewBufferWriter()
	writerErr, _ := utils.NewBufferWriter()
	logger := NewLogger(writerOut, writerErr, nil, false)
	assert.NotNil(t, logger)
}

func TestFileCore(t *testing.T) {
	tempDir := t.TempDir()
	filePath := tempDir + "/log-file.txt"
	file, err := os.Create(filePath)
	assert.NoError(t, err)

	writerOut, _ := utils.NewBufferWriter()
	writerErr, _ := utils.NewBufferWriter()
	logger := NewLogger(writerOut, writerErr, file, false)

	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert, info is logged without prefix
	expected := "DEBUG\tDebug msg\nINFO\tInfo msg\nWARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expected, utils.GetFileContent(filePath))
}

func TestConsoleCoreVerboseFalse(t *testing.T) {
	writerOut, bufferOut := utils.NewBufferWriter()
	writerErr, bufferErr := utils.NewBufferWriter()
	logger := NewLogger(writerOut, writerErr, nil, false)

	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Flush
	err := writerOut.Flush()
	assert.NoError(t, err)
	err = writerErr.Flush()
	assert.NoError(t, err)

	// Assert
	expectedOut := "Info msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, bufferOut.String())
	assert.Equal(t, expectedErr, bufferErr.String())
}

func TestConsoleCoreVerboseTrue(t *testing.T) {
	writerOut, bufferOut := utils.NewBufferWriter()
	writerErr, bufferErr := utils.NewBufferWriter()
	logger := NewLogger(writerOut, writerErr, nil, true)
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Flush
	err := writerOut.Flush()
	assert.NoError(t, err)
	err = writerErr.Flush()
	assert.NoError(t, err)

	// Assert
	expectedOut := "Debug msg\nInfo msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, bufferOut.String())
	assert.Equal(t, expectedErr, bufferErr.String())
}

func TestToInfoWriter(t *testing.T) {
	writerOut, bufferOut := utils.NewBufferWriter()
	writerErr, bufferErr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(writerOut, writerErr, nil, false)
	_, err := ToInfoWriter(logger).Write([]byte("test\n"))

	// Flush
	assert.NoError(t, err)
	err = writerOut.Flush()
	assert.NoError(t, err)
	err = writerErr.Flush()
	assert.NoError(t, err)

	assert.Equal(t, "test\n", bufferOut.String())
	assert.Equal(t, "", bufferErr.String())
}

func TestToWarnWriter(t *testing.T) {
	writerOut, bufferOut := utils.NewBufferWriter()
	writerErr, bufferErr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(writerOut, writerErr, nil, false)
	_, err := ToWarnWriter(logger).Write([]byte("test\n"))

	// Flush
	assert.NoError(t, err)
	err = writerOut.Flush()
	assert.NoError(t, err)
	err = writerErr.Flush()
	assert.NoError(t, err)

	assert.Equal(t, "", bufferOut.String())
	assert.Equal(t, "test\n", bufferErr.String())
}
