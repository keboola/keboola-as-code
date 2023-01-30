//nolint:forbidigo
package cli

import (
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/google/shlex"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
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

	loadArgsFn := func(t *runner.Test) []string {
		// Load command arguments from file
		argsFile, err := t.TestDirFS().ReadFile(filesystem.NewFileDef("args"))
		if err != nil {
			t.T().Fatalf(`cannot open "%s" test file %s`, "args", err)
		}

		// Load and parse command arguments
		argsStr := strings.TrimSpace(argsFile.Content)
		argsStr = testhelper.MustReplaceEnvsString(argsStr, t.EnvProvider())
		args, err := shlex.Split(argsStr)
		if err != nil {
			t.T().Fatalf(`Cannot parse args "%s": %s`, argsStr, err)
		}
		return args
	}

	r.ForEachTest(func(test *runner.Test) {
		test.Run(
			runner.WithCopyInToWorkingDir(),
			runner.WithInitProjectState(),
			runner.WithAddEnvVarsFromFile(),
			runner.WithRunCLIBinary(binaryPath, loadArgsFn),
			runner.WithAssertProjectState(),
			runner.WithAssertDirContent(),
		)
	})
}
