package cli

import (
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

func TestRootSubCommands(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)

	// Map commands to names
	var names []string
	for _, cmd := range root.cmd.Commands() {
		names = append(names, cmd.Name())
	}

	// Assert
	assert.Equal(t, []string{
		"init",
	}, names)
}

func TestRootCmdPersistentFlags(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)

	// Map flags to names
	var names []string
	root.cmd.PersistentFlags().VisitAll(func(flag *pflag.Flag) {
		names = append(names, flag.Name)
	})

	// Assert
	expected := []string{
		"api-url",
		"dir",
		"help",
		"log-file",
		"token",
		"verbose",
	}
	assert.Equal(t, expected, names)
}

func TestRootCmdFlags(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)

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
	logger, writer, buffer := utils.NewDebugLogger()
	root := NewRootCommand(writer, writer)

	// Execute
	root.logger = logger
	root.Execute()

	// Assert
	err := writer.Flush()
	assert.NoError(t, err)
	assert.Contains(t, buffer.String(), "Available Commands:")

}

func TestTearDownRemoveLogFile(t *testing.T) {
	tempDir := t.TempDir()
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)

	root.logFilePath = filepath.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.logFilePath)
	root.logFileClear = false // <<<<<
	root.tearDown()
	assert.FileExists(t, root.logFilePath)
}

func TestTearDownKeepLogFile(t *testing.T) {
	tempDir := t.TempDir()
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)

	root.logFilePath = filepath.Join(tempDir, "log-file.txt")
	root.logFile, _ = os.Create(root.logFilePath)
	root.logFileClear = true // <<<<<
	root.tearDown()
	assert.NoFileExists(t, root.logFilePath)
}

func TestInit(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	assert.False(t, root.initialized)
	assert.Nil(t, root.logger)
	assert.Empty(t, root.params)
	err := root.init()
	assert.NoError(t, err)
	assert.True(t, root.initialized)
	assert.NotNil(t, root.logger)
	assert.NotEmpty(t, root.params.WorkingDirectory)
}

func TestLogVersion(t *testing.T) {
	logger, writer, buffer := utils.NewDebugLogger()
	root := NewRootCommand(writer, writer)

	// Log version
	err := root.init()
	assert.NoError(t, err)
	root.logger = logger
	root.logVersion()

	// Assert
	err = writer.Flush()
	assert.NoError(t, err)
	assert.Regexp(
		t,
		`^`+
			`DEBUG  Version:.*\n`+
			`DEBUG  Git commit:.*\n`+
			`DEBUG  Build date:.*\n`+
			`DEBUG  Go version:\s+`+regexp.QuoteMeta(runtime.Version())+`\n`+
			`DEBUG  Os/Arch:\s+`+regexp.QuoteMeta(runtime.GOOS)+`/`+regexp.QuoteMeta(runtime.GOARCH)+`\n`+
			`$`,
		buffer.String(),
	)
}

func TestLogCommand(t *testing.T) {
	logger, writer, buffer := utils.NewDebugLogger()
	root := NewRootCommand(writer, writer)

	// Log version
	err := root.init()
	assert.NoError(t, err)
	root.logger = logger
	root.logCommand()

	// Assert
	err = writer.Flush()
	assert.NoError(t, err)
	assert.Regexp(t, `^DEBUG  Running command \[.+\]\n$`, buffer.String())
}

func TestGetLogFileTempFile(t *testing.T) {
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.True(t, strings.HasPrefix(root.logFilePath, os.TempDir()+"/"))
	assert.True(t, root.logFileClear)
}

func TestGetLogFileFromFlags(t *testing.T) {
	tempDir := t.TempDir()
	writer, _ := utils.NewBufferWriter()
	root := NewRootCommand(writer, writer)
	root.flags.LogFilePath = filepath.Join(tempDir, "log-file.txt")
	file, err := root.getLogFile()
	assert.NoError(t, err)
	assert.NotNil(t, file)
	assert.Equal(t, root.flags.LogFilePath, root.logFilePath)
	assert.False(t, root.logFileClear)
}
