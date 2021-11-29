package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	interactivePrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/interactive"
	nopPrompt "github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRootSubCommands(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())

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
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())

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
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())

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
	root, out := newTestRootCommand(testhelper.NewMemoryFs())

	// Execute
	root.Logger = zap.NewNop().Sugar()
	assert.Equal(t, 0, root.Execute())
	assert.Contains(t, out.String(), "Available Commands:")
}

func TestTearDownRemoveLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())

	root.Options.LogFilePath = ""
	root.setupLogger()
	assert.True(t, root.logFile.IsTemp())

	assert.FileExists(t, root.logFile.Path())
	root.tearDown(0)
	assert.NoFileExists(t, root.logFile.Path())
}

func TestTearDownKeepLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())
	tempDir := t.TempDir()

	root.Options.LogFilePath = filepath.Join(tempDir, "log-file.txt")
	root.setupLogger()
	assert.False(t, root.logFile.IsTemp())
	assert.Equal(t, root.logFile.Path(), root.Options.LogFilePath)

	assert.FileExists(t, root.Options.LogFilePath)
	root.tearDown(0)
	assert.FileExists(t, root.Options.LogFilePath)
}

func TestGetLogFileTempFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())
	root.Options.LogFilePath = ""
	root.setupLogger()
	assert.True(t, root.logFile.IsTemp())

	// Linux returns temp dir without last separator, MacOs with last separator.
	// ... so we need to make sure there is only one separator at the end.
	tempDir := strings.TrimRight(os.TempDir(), string(os.PathSeparator)) + string(os.PathSeparator) // nolint forbidigo
	assert.True(t, strings.HasPrefix(root.logFile.Path(), tempDir))
	assert.True(t, root.logFile.IsTemp())
	assert.NoError(t, root.logFile.File().Close())
}

func TestGetLogFileFromFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(testhelper.NewMemoryFs())

	// Note: log file can be outside project directory, so it is NOT using virtual filesystem
	tempDir := t.TempDir()
	root.Options.LogFilePath = filesystem.Join(tempDir, "log-file.txt")
	root.setupLogger()
	assert.Equal(t, root.Options.LogFilePath, root.Options.LogFilePath)
	assert.False(t, root.logFile.IsTemp())
	assert.NoError(t, root.logFile.File().Close())
}

func newTestRootCommand(fs filesystem.Fs) (*RootCommand, *utils.Writer) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	fsFactory := func(logger *zap.SugaredLogger, workingDir string) (filesystem.Fs, error) {
		return fs, nil
	}
	return NewRootCommand(in, out, out, nopPrompt.New(), env.Empty(), fsFactory), out
}

func newTestRootCommandWithTty(tty *os.File, fs filesystem.Fs) *RootCommand {
	prompt := interactivePrompt.New(tty, tty, tty)
	fsFactory := func(logger *zap.SugaredLogger, workingDir string) (filesystem.Fs, error) {
		return fs, nil
	}
	return NewRootCommand(tty, tty, tty, prompt, env.Empty(), fsFactory)
}
