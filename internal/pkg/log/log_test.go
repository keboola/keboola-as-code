// nolint: forbidigo
package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestNewLogger(t *testing.T) {
	t.Parallel()
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, false)
	assert.NotNil(t, logger)
}

func TestFileCore(t *testing.T) {
	t.Parallel()
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
	assert.NoError(t, file.Close())

	// Assert, all levels logged with the level prefix
	expected := "DEBUG\tDebug msg\nINFO\tInfo msg\nWARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expected, testhelper.GetFileContent(filePath))
}

func TestConsoleCoreVerboseFalse(t *testing.T) {
	t.Parallel()
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, false)

	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert
	// info      -> stdout
	// warn, err -> stderr
	expectedOut := "Info msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}

func TestConsoleCoreVerboseTrue(t *testing.T) {
	t.Parallel()
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, nil, true)
	logger.Debug("Debug msg")
	logger.Info("Info msg")
	logger.Warn("Warn msg")
	logger.Error("Error msg")

	// Assert
	// debug (verbose), info -> stdout
	// warn, err             -> stderr
	expectedOut := "DEBUG\tDebug msg\nINFO\tInfo msg\n"
	expectedErr := "WARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}

func TestToInfoWriter(t *testing.T) {
	t.Parallel()
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(stdout, stderr, nil, false)
	_, err := ToInfoWriter(logger).Write([]byte("test\n"))
	assert.NoError(t, err)

	// Assert, written to stdout
	assert.Equal(t, "test\n", stdout.String())
	assert.Equal(t, "", stderr.String())
}

func TestToWarnWriter(t *testing.T) {
	t.Parallel()
	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()

	// Write
	logger := NewLogger(stdout, stderr, nil, false)
	_, err := ToWarnWriter(logger).Write([]byte("test\n"))
	assert.NoError(t, err)

	// Assert, written to stderr
	assert.Equal(t, "", stdout.String())
	assert.Equal(t, "test\n", stderr.String())
}

func TestWriteStringNoErrIndent1(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := os.Create(filePath)
	assert.NoError(t, err)

	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, file, false)

	writer := ToInfoWriter(logger)
	writer.WriteStringNoErrIndent1("test")
	assert.NoError(t, file.Close())

	// Assert, all levels logged with the level prefix
	expected := "INFO\t  test\n"
	assert.Equal(t, expected, testhelper.GetFileContent(filePath))
}

func TestWriteStringNoErrIndent(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := os.Create(filePath)
	assert.NoError(t, err)

	stdout := utils.NewBufferWriter()
	stderr := utils.NewBufferWriter()
	logger := NewLogger(stdout, stderr, file, false)

	writer := ToInfoWriter(logger)
	writer.WriteStringNoErrIndent("test", 3)
	writer.WriteStringNoErrIndent("test", 2)
	assert.NoError(t, file.Close())

	// Assert, all levels logged with the level prefix
	expected := "INFO\t      test\nINFO\t    test\n"
	assert.Equal(t, expected, testhelper.GetFileContent(filePath))
}
