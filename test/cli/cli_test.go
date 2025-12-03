//nolint:forbidigo
package cli

import (
	"strings"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

// TestCliE2E runs one CLI functional test per each subdirectory.
func TestCliE2E(t *testing.T) {
	t.Parallel()

	binaryPath := testhelper.CompileBinary(t, "cli", "build-local")

	// List of tests to skip
	skipTests := map[string]bool{
		"push/config-data-gateway-push-and-push-dry-run": true,
		"push/empty-data-gateway":                        true,
	}

	runner.
		NewRunner(t).
		ForEachTest(func(test *runner.Test) {
			// Skip specific tests based on test name
			testName := test.T().Name()
			// Test name format is "TestCliE2E/push/empty-data-gateway" (Unix)
			// or "TestCliE2E/push\empty-data-gateway" (Windows)
			// Normalize path separators to forward slashes for cross-platform compatibility
			normalizedTestName := strings.ReplaceAll(testName, "\\", "/")
			// Extract the part after the first "/" (which is the test function name)
			parts := strings.Split(normalizedTestName, "/")
			if len(parts) > 1 {
				relativePath := strings.Join(parts[1:], "/")
				if skipTests[relativePath] {
					test.T().Skipf("Skipping test: %s", relativePath)
					return
				}
			}

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
