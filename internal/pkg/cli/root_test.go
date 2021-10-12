package cli

import (
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRootSubCommands(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Map commands to names
	var names []string
	for _, cmd := range root.cmd.Commands() {
		names = append(names, cmd.Name())
	}

	// Assert
	assert.Equal(t, []string{
		"create",
		"diff",
		"encrypt",
		"fix-paths",
		"init",
		"persist",
		"pull",
		"push",
		"status",
		"validate",
		"workflows",
	}, names)
}

func TestRootCmdPersistentFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Map flags to names
	var names []string
	root.cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	// Assert
	expected := []string{
		"help",
		"log-file",
		"storage-api-token",
		"verbose",
		"verbose-api",
		"working-dir",
	}
	assert.Equal(t, expected, names)
}

func TestRootCmdFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Map flags to names
	var names []string
	root.cmd.Flags().VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	// Assert
	expected := []string{
		"version",
	}
	assert.Equal(t, expected, names)
}

func TestExecute(t *testing.T) {
	t.Parallel()
	root, out := newTestRootCommand()

	// Execute
	root.logger = zap.NewNop().Sugar()
	assert.Equal(t, 0, root.Execute())
	assert.Contains(t, out.String(), "Available Commands:")
}

func TestTearDownRemoveLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Note: log file can be outside project directory, so it is NOT using virtual filesystem
	tempDir := t.TempDir()
	root.options.LogFilePath = filesystem.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.options.LogFilePath) // nolint: forbidigo
	root.logFileClear = false                             // <<<<<
	root.tearDown()
	assert.FileExists(t, root.options.LogFilePath)
}

func TestTearDownKeepLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Note: log file can be outside project directory, so it is NOT using virtual filesystem
	tempDir := t.TempDir()
	root.options.LogFilePath = filesystem.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.options.LogFilePath) // nolint: forbidigo
	root.logFileClear = true                              // <<<<<
	root.tearDown()
	assert.NoFileExists(t, root.options.LogFilePath)
}

func TestInit(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()
	assert.False(t, root.initialized)
	assert.Nil(t, root.logger)
	err := root.init(root.cmd)
	assert.NoError(t, err)
	assert.True(t, root.initialized)
	assert.NotNil(t, root.logger)
}

func TestLogVersion(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()
	logger, out := utils.NewDebugLogger()

	// Log version
	err := root.init(root.cmd)
	assert.NoError(t, err)
	root.logger = logger
	root.logDebugInfo()

	// Assert
	assert.Regexp(
		t,
		`^`+
			`DEBUG  Version:.*\n`+
			`DEBUG  Git commit:.*\n`+
			`DEBUG  Build date:.*\n`+
			`DEBUG  Go version:\s+`+regexp.QuoteMeta(runtime.Version())+`\n`+
			`DEBUG  Os/Arch:\s+`+regexp.QuoteMeta(runtime.GOOS)+`/`+regexp.QuoteMeta(runtime.GOARCH)+`\n`+
			`DEBUG  Running command \[.+\]\n`+
			`DEBUG  Parsed options: .+\n`+
			`$`,
		out.String(),
	)
}

func TestGetLogFileTempFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)

	// Linux returns temp dir without last separator, MacOs with last separator.
	// ... so we need to make sure there is only one separator at the end.
	tempDir := strings.TrimRight(os.TempDir(), string(os.PathSeparator)) + string(os.PathSeparator)
	assert.True(t, strings.HasPrefix(root.options.LogFilePath, tempDir))
	assert.True(t, root.logFileClear)
}

func TestGetLogFileFromFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand()

	// Note: log file can be outside project directory, so it is NOT using virtual filesystem
	tempDir := t.TempDir()
	root.options.LogFilePath = filesystem.Join(tempDir, "log-file.txt")
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, root.options.LogFilePath, root.options.LogFilePath)
	assert.False(t, root.logFileClear)
}
