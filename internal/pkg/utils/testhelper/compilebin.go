package testhelper

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

// CompileBinary compiles a binary used in the test by running a make command.
func CompileBinary(t *testing.T, binaryName string, makeCommand string) string {
	t.Helper()

	// Get project dir, to run "make ..."
	_, thisFile, _, _ := runtime.Caller(0)                       //nolint:dogsled
	thisDir := filepath.Dir(thisFile)                            //nolint:forbidigo
	projectDir := filepath.Join(thisDir, "..", "..", "..", "..") //nolint:forbidigo

	// Compose binary path
	tmpDir := t.TempDir()
	binaryPath := filesystem.Join(tmpDir, binaryName)
	if runtime.GOOS == "windows" {
		binaryPath += `.exe`
	}

	// Envs
	envs, err := env.FromOs()
	require.NoError(t, err)
	envs.Set("BUILD_TARGET_PATH", binaryPath)

	// Build cmd
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("make", makeCommand)
	cmd.Dir = projectDir
	cmd.Env = envs.ToSlice()
	cmd.Stdout = io.MultiWriter(&stdout, VerboseStdout())
	cmd.Stderr = io.MultiWriter(&stderr, VerboseStderr())

	// Run
	t.Logf(`compiling "%s" binary to "%s" ...`, binaryName, binaryPath)
	if err := cmd.Run(); err != nil {
		t.Fatalf("compilation failed: %s\nSTDOUT:\n%s\n\nSTDERR:\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}
	if _, err := os.Stat(binaryPath); err != nil { //nolint:forbidigo
		t.Fatalf("compilation failed, binary not found: %s\nSTDOUT:\n%s\n\nSTDERR:\n%s\n", err, stdout.Bytes(), stderr.Bytes())
	}
	t.Logf(`compilation "%s" done`, binaryName)

	return binaryPath
}
