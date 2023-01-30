//nolint:forbidigo
package cli

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

// TestCliE2E runs one CLI functional test per each subdirectory.
func TestCliE2E(t *testing.T) {
	t.Parallel()

	_, testFile, _, _ := runtime.Caller(0)
	testsDir := filepath.Dir(testFile)
	rootDir := filepath.Join(testsDir, "..", "..")

	r := runner.NewRunner(t, testsDir)
	binaryPath := r.CompileBinary(
		rootDir,
		"bin_func_tests",
		"TARGET_PATH",
		"build-local",
	)

	r.ForEachTest(func(test *runner.Test) {
		test.Run(
			runner.WithCopyInToWorkingDir(),
			runner.WithInitProjectState(),
			runner.WithAddEnvVarsFromFile(),
			runner.WithLoadArgsFile(),
			runner.WithRunCLIBinary(binaryPath),
			runner.WithAssertProjectState(),
			runner.WithAssertDirContent(),
		)
	})
}
