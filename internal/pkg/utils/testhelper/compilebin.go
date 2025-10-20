package testhelper

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
)

// CompileBinary compiles a binary used in the test by running a task command.
func CompileBinary(t *testing.T, binaryName string, taskCommand string) string {
	t.Helper()

	// Compilation can be skipped by providing path to the binary.
	// It is used in CI to cache test results.
	pathEnv := "TEST_BINARY_" + strings.ToUpper(strings.ReplaceAll(binaryName, "-", "_"))
	hashEnv := pathEnv + "_HASH"
	path, ok1 := os.LookupEnv(pathEnv)
	hash, ok2 := os.LookupEnv(hashEnv) // hash ENV (each ENV) modification invalidates the test cache
	if ok1 && ok2 && path != "" && hash != "" {
		t.Logf(`"%s" = "%s" (%s)`, pathEnv, path, hash)
		_, err := os.Stat(path) //nolint:forbidigo
		require.NoError(t, err)
		return path
	} else {
		t.Logf(`no "%s" / "%s" envs found`, pathEnv, hashEnv)
	}

	// Get project dir, to run "task ..."
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
	cmd := exec.CommandContext(t.Context(), "task", taskCommand)
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
