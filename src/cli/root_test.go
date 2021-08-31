package cli

import (
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/interaction"
	"keboola-as-code/src/utils"
)

func TestRootSubCommands(t *testing.T) {
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

	// Map commands to names
	var names []string
	for _, cmd := range root.cmd.Commands() {
		names = append(names, cmd.Name())
	}

	// Assert
	assert.Equal(t, []string{
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
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

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
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

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
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))
	in := utils.NewBufferReader()
	logger, out := utils.NewDebugLogger()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

	// Execute
	root.logger = logger
	root.Execute()
	assert.Contains(t, out.String(), "Available Commands:")
}

func TestTearDownRemoveLogFile(t *testing.T) {
	tempDir := t.TempDir()
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

	root.options.LogFilePath = filepath.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.options.LogFilePath)
	root.logFileClear = false // <<<<<
	root.tearDown()
	assert.FileExists(t, root.options.LogFilePath)
}

func TestTearDownKeepLogFile(t *testing.T) {
	tempDir := t.TempDir()
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

	root.options.LogFilePath = filepath.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.options.LogFilePath)
	root.logFileClear = true // <<<<<
	root.tearDown()
	assert.NoFileExists(t, root.options.LogFilePath)
}

func TestInit(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))
	assert.False(t, root.initialized)
	assert.Nil(t, root.logger)
	err := root.init(root.cmd)
	assert.NoError(t, err)
	assert.True(t, root.initialized)
	assert.NotNil(t, root.logger)
	assert.NotEmpty(t, root.options.WorkingDirectory)
}

func TestLogVersion(t *testing.T) {
	tempDir := t.TempDir()
	assert.NoError(t, os.Chdir(tempDir))
	in := utils.NewBufferReader()
	logger, out := utils.NewDebugLogger()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))

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
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.True(t, strings.HasPrefix(root.options.LogFilePath, os.TempDir()+"/"))
	assert.True(t, root.logFileClear)
}

func TestGetLogFileFromFlags(t *testing.T) {
	tempDir := t.TempDir()
	in := utils.NewBufferReader()
	out := utils.NewBufferWriter()
	root := NewRootCommand(in, out, out, interaction.NewPrompt(in, out, out))
	root.options.LogFilePath = filepath.Join(tempDir, "log-file.txt")
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, root.options.LogFilePath, root.options.LogFilePath)
	assert.False(t, root.logFileClear)
}
