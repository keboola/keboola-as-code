// nolint: forbidigo
package log

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestCliLogger_New(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, LogFormatConsole, false)
	assert.NotNil(t, logger)
}

func TestCliLogger_File(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := NewLogFile(filePath)
	require.NoError(t, err)

	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, file, LogFormatConsole, false)

	logger.Debug(t.Context(), "Debug msg")
	logger.Info(t.Context(), "Info msg")
	logger.Warn(t.Context(), "Warn msg")
	logger.Error(t.Context(), "Error msg")
	require.NoError(t, file.File().Close())

	// Assert, all levels logged with the level prefix
	expected := `
{"level":"debug","time":"%s","message":"Debug msg"}
{"level":"info","time":"%s","message":"Info msg"}
{"level":"warn","time":"%s","message":"Warn msg"}
{"level":"error","time":"%s","message":"Error msg"}
`

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	wildcards.Assert(t, expected, string(content))
}

func TestCliLogger_VerboseFalse(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, LogFormatConsole, false)
	// Check that context attributes don't appear in stdout/stderr.
	ctx := ctxattr.ContextWith(t.Context(), attribute.String("extra", "value"))

	logger.Debug(ctx, "Debug msg")
	logger.Info(ctx, "Info msg")
	logger.Warn(ctx, "Warn msg")
	logger.Error(ctx, "Error msg")

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
	logger := NewCliLogger(stdout, stderr, nil, LogFormatConsole, true)
	// Check that context attributes don't appear in stdout/stderr.
	ctx := ctxattr.ContextWith(t.Context(), attribute.String("extra", "value"))

	logger.Debug(ctx, "Debug msg")
	logger.Info(ctx, "Info msg")
	logger.Warn(ctx, "Warn msg")
	logger.Error(ctx, "Error msg")

	// Assert
	// debug (verbose), info -> stdout
	// warn, err             -> stderr
	expectedOut := "DEBUG\tDebug msg\nINFO\tInfo msg\n"
	expectedErr := "WARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}

func TestCliLogger_JSONVerboseFalse(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, LogFormatJSON, false)
	ctx := t.Context()

	logger.Debug(ctx, "Debug msg")
	logger.Info(ctx, "Info msg")
	logger.Warn(ctx, "Warn msg")
	logger.Error(ctx, "Error msg")

	// Assert
	// info      -> stdout
	// warn, err -> stderr
	expectedOut := `
{"level":"info","time":"%s","message":"Info msg"}
`
	expectedErr := `
{"level":"warn","time":"%s","message":"Warn msg"}
{"level":"error","time":"%s","message":"Error msg"}
`

	wildcards.Assert(t, expectedOut, stdout.String())
	wildcards.Assert(t, expectedErr, stderr.String())
}

func TestCliLogger_JSONVerboseTrue(t *testing.T) {
	t.Parallel()
	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, nil, LogFormatJSON, true)
	ctx := t.Context()

	logger.Debug(ctx, "Debug msg")
	logger.Info(ctx, "Info msg")
	logger.Warn(ctx, "Warn msg")
	logger.Error(ctx, "Error msg")

	// Assert
	// debug (verbose), info -> stdout
	// warn, err             -> stderr
	expectedOut := `
{"level":"debug","time":"%s","message":"Debug msg"}
{"level":"info","time":"%s","message":"Info msg"}
`
	expectedErr := `
{"level":"warn","time":"%s","message":"Warn msg"}
{"level":"error","time":"%s","message":"Error msg"}
`

	wildcards.Assert(t, expectedOut, stdout.String())
	wildcards.Assert(t, expectedErr, stderr.String())
}

func TestCliLogger_AttributeReplace(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := NewLogFile(filePath)
	require.NoError(t, err)

	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, file, LogFormatConsole, true)

	ctx := ctxattr.ContextWith(t.Context(), attribute.String("extra", "value"), attribute.Int("count", 4))

	logger.Debug(ctx, "Debug msg <extra> (<count>)")
	logger.Info(ctx, "Info msg <extra> (<count>)")
	logger.Warn(ctx, "Warn msg <extra> (<count>)")
	logger.Error(ctx, "Error msg <extra> (<count>)")
	logger.Debugf(ctx, "Debug %s <extra> (<count>)", "message")
	logger.Infof(ctx, "Info %s <extra> (<count>)", "message")
	logger.Warnf(ctx, "Warn %s <extra> (<count>)", "message")
	logger.Errorf(ctx, "Error %s <extra> (<count>)", "message")
	require.NoError(t, file.File().Close())

	// Assert, all levels logged with the level prefix
	expected := `
{"level":"debug","time":"%s","message":"Debug msg value (4)","count":4,"extra":"value"}
{"level":"info","time":"%s","message":"Info msg value (4)","count":4,"extra":"value"}
{"level":"warn","time":"%s","message":"Warn msg value (4)","count":4,"extra":"value"}
{"level":"error","time":"%s","message":"Error msg value (4)","count":4,"extra":"value"}
{"level":"debug","time":"%s","message":"Debug message value (4)","count":4,"extra":"value"}
{"level":"info","time":"%s","message":"Info message value (4)","count":4,"extra":"value"}
{"level":"warn","time":"%s","message":"Warn message value (4)","count":4,"extra":"value"}
{"level":"error","time":"%s","message":"Error message value (4)","count":4,"extra":"value"}
`

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	wildcards.Assert(t, expected, string(content))

	expectedOut := "DEBUG\tDebug msg value (4)\nINFO\tInfo msg value (4)\nDEBUG\tDebug message value (4)\nINFO\tInfo message value (4)\n"
	expectedErr := "WARN\tWarn msg value (4)\nERROR\tError msg value (4)\nWARN\tWarn message value (4)\nERROR\tError message value (4)\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}

func TestCliLogger_WithComponent(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "log-file.txt")
	file, err := NewLogFile(filePath)
	require.NoError(t, err)

	stdout := ioutil.NewAtomicWriter()
	stderr := ioutil.NewAtomicWriter()
	logger := NewCliLogger(stdout, stderr, file, LogFormatConsole, true)

	logger = logger.WithComponent("component").WithComponent("subcomponent")

	ctx := t.Context()

	logger.Debug(ctx, "Debug msg")
	logger.Info(ctx, "Info msg")
	logger.Warn(ctx, "Warn msg")
	logger.Error(ctx, "Error msg")
	require.NoError(t, file.File().Close())

	// Assert, all levels logged with the level prefix
	expected := `
{"level":"debug","time":"%s","message":"Debug msg","component":"component.subcomponent"}
{"level":"info","time":"%s","message":"Info msg","component":"component.subcomponent"}
{"level":"warn","time":"%s","message":"Warn msg","component":"component.subcomponent"}
{"level":"error","time":"%s","message":"Error msg","component":"component.subcomponent"}
`

	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	wildcards.Assert(t, expected, string(content))

	expectedOut := "DEBUG\tDebug msg\nINFO\tInfo msg\n"
	expectedErr := "WARN\tWarn msg\nERROR\tError msg\n"
	assert.Equal(t, expectedOut, stdout.String())
	assert.Equal(t, expectedErr, stderr.String())
}
