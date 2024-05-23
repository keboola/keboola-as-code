//nolint:forbidigo
package cli

import (
	"os"
	"testing"

	// enables caching of binary.
	_ "github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

// TestCliE2E runs one CLI functional test per each subdirectory.
func TestCliE2E(t *testing.T) {
	t.Parallel()

	binaryPath := os.Getenv("TEST_BINARY_PATH")
	if binaryPath == "" {
		binaryPath = testhelper.CompileBinary(t, "cli", "build-local")
	}

	runner.
		NewRunner(t).
		ForEachTest(func(test *runner.Test) {
			test.Run(
				runner.WithCopyInToWorkingDir(),
				runner.WithInitProjectState(),
				runner.WithAddEnvVarsFromFile(),
				runner.WithRunCLIBinary(binaryPath),
				runner.WithAssertProjectState(),
				runner.WithAssertDirContent(),
			)
		})
}
