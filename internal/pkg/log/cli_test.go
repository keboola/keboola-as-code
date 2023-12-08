// nolint: forbidigo
package log

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestCliLogger_New(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, false)
	assert.NotNil(t, logger)
}

func TestCliLogger_File(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := NewLogFile(filePath)
	assert.NoError(t, err)

	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, file, false)

	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")
	assert.NoError(t, file.File().Close())

	// Assert, all levels logged with the level prefix
	expected := `
{"level":"debug","time":"%s","message":"Debug msg"}
{"level":"info","time":"%s","message":"Info msg"}
{"level":"warn","time":"%s","message":"Warn msg"}
{"level":"error","time":"%s","message":"Error msg"}
`

	content, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	wildcards.Assert(t, expected, string(content))
}

func TestCliLogger_VerboseFalse(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, false)

	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Assert
	// info      -> stdout
	// warn, err -> stderr
	expectedOut := "Info msg\n"
	expectedErr := "Warn msg\nError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}

func TestCliLogger_VerboseTrue(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, true)
	logger.DebugCtx(context.Background(), "Debug msg")
	logger.InfoCtx(context.Background(), "Info msg")
	logger.WarnCtx(context.Background(), "Warn msg")
	logger.ErrorCtx(context.Background(), "Error msg")

	// Assert
	// debug (verbose), info -> stdout
	// warn, err             -> stderr
	expectedOut := "DEBUG\tDebug msg\nINFO\tInfo msg\n"
	expectedErr := "WARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}
