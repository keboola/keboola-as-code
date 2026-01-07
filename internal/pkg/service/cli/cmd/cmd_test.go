package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestCliSubCommands(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	// Map commands to names, skip hidden
	var names []string
	for _, cmd := range root.Commands() {
		if !cmd.Hidden {
			names = append(names, cmd.Name())
		}
	}

	// Assert
	assert.Equal(t, []string{
		"status",
		"sync",
		"ci",
		"local",
		"remote",
		"dbt",
		"template",
		"llm",
	}, names)
}

func TestCliSubCommandsAndAliases(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	// Map commands to names
	cmds := root.Commands()
	names := make([]string, 0, len(cmds))
	for _, cmd := range cmds {
		names = append(names, cmd.Name())
	}

	// Assert
	assert.Equal(t, []string{
		"status",
		"sync",
		"ci",
		"local",
		"remote",
		"dbt",
		"template",
		"llm",
		"i",
		"d",
		"pl",
		"ph",
		"v",
		"pt",
		"c",
		"e",
		"init",
		"diff",
		"pull",
		"push",
		"validate",
		"persist",
		"create",
		"encrypt",
		"use",
		"t",
		"r",
		"repo",
		"table",
	}, names)
}

func TestCliCmdPersistentFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	// Map flags to names
	var names []string
	root.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	// Assert
	expected := []string{
		"help",
		"log-file",
		"log-format",
		"non-interactive",
		"verbose",
		"verbose-api",
		"version-check",
		"working-dir",
	}
	assert.Equal(t, expected, names)
}

func TestCliCmdFlags(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	// Map flags to names
	var names []string
	root.Flags().VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	var persistentFlags []string
	root.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		persistentFlags = append(persistentFlags, flag.Name)
	})

	persistentExpected := []string{
		"help",
		"log-file",
		"log-format",
		"non-interactive",
		"verbose",
		"verbose-api",
		"version-check",
		"working-dir",
	}

	assert.Equal(t, persistentExpected, persistentFlags)
	// Assert
	expected := []string{
		"version",
	}
	assert.Equal(t, expected, names)
}

func TestExecute(t *testing.T) {
	t.Parallel()
	root, out := newTestRootCommand(aferofs.NewMemoryFs())

	// Execute
	root.logger = log.NewNopLogger()
	assert.Equal(t, 0, root.Execute())
	assert.Contains(t, out.String(), "Available Commands:")
}

func TestTearDown_RemoveLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	root.globalFlags.LogFile.Value = ""
	root.setupLogger()
	assert.True(t, root.logFile.IsTemp())

	assert.FileExists(t, root.logFile.Path())
	root.tearDown(0, nil)
	assert.NoFileExists(t, root.logFile.Path())
}

func TestTearDown_KeepLogFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())
	tempDir := t.TempDir()

	root.globalFlags.LogFile.Value = filepath.Join(tempDir, "log-file.txt") // nolint: forbidigo
	root.setupLogger()
	assert.False(t, root.logFile.IsTemp())
	assert.Equal(t, root.logFile.Path(), root.globalFlags.LogFile.Value)

	assert.FileExists(t, root.globalFlags.LogFile.Value)
	root.tearDown(0, nil)
	assert.FileExists(t, root.globalFlags.LogFile.Value)
}

func TestTearDown_Panic(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())
	root.logger = logger
	exitCode := root.tearDown(0, errors.New("panic error"))
	assert.Equal(t, 1, exitCode)
	expected := `
DEBUG  Unexpected panic: panic error
%A
INFO  
---------------------------------------------------
Keboola CLI had a problem and crashed.

To help us diagnose the problem you can send us a crash report.

Please run the command again with the flag "--log-file <path>" to generate a log file.

Then please submit email to "support@keboola.com" and include the log file as an attachment.

We take privacy seriously, and do not perform any automated log file collection.

Thank you kindly!
`
	wildcards.Assert(t, expected, logger.AllMessagesTxt())
}

func TestGetLogFileTempFile(t *testing.T) {
	t.Parallel()
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())
	root.globalFlags.LogFile.Value = ""
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
	root, _ := newTestRootCommand(aferofs.NewMemoryFs())

	// Note: log file can be outside project directory, so it is NOT using virtual filesystem
	tempDir := t.TempDir()
	root.globalFlags.LogFile.Value = filesystem.Join(tempDir, "log-file.txt")
	root.setupLogger()
	assert.False(t, root.logFile.IsTemp())
	assert.NoError(t, root.logFile.File().Close())
}

func newTestRootCommand(fs filesystem.Fs) (*RootCommand, *ioutil.AtomicWriter) {
	in := ioutil.NewBufferedReader()
	out := ioutil.NewAtomicWriter()
	fsFactory := func(_ context.Context, opts ...filesystem.Option) (filesystem.Fs, error) {
		return fs, nil
	}

	envs := env.Empty()

	root := NewRootCommand(in, out, out, envs, fsFactory)
	if root.Context() == nil {
		root.SetContext(context.Background())
	}

	return root, out
}
