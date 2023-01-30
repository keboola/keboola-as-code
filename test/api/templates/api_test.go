//nolint:forbidigo
package api

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

// TestTemplatesApiE2E runs one Templates API functional test per each subdirectory.
func TestTemplatesApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	_, testFile, _, _ := runtime.Caller(0)
	testsDir := filepath.Dir(testFile)
	rootDir := filepath.Join(testsDir, "..", "..", "..")

	r := runner.NewRunner(t, testsDir)
	binaryPath := r.CompileBinary(
		rootDir,
		"templates-api",
		"TEMPLATES_API_BUILD_TARGET_PATH",
		"build-templates-api",
	)

	setupAPIServerFn := func(test *runner.Test) ([]string, map[string]string) {
		var repositories string
		if test.TestDirFS().Exists("repository") {
			repositories = fmt.Sprintf("keboola|file://%s", filepath.Join(test.TestDirFS().BasePath(), "repository"))
		} else {
			repositories = "keboola|https://github.com/keboola/keboola-as-code-templates.git|main"
		}
		addArgs := []string{fmt.Sprintf("--repositories=%s", repositories)}

		addEnvs := map[string]string{
			"TEMPLATES_API_ETCD_ENABLED":   "true",
			"TEMPLATES_API_ETCD_NAMESPACE": idgenerator.EtcdNamespaceForTest(),
			"TEMPLATES_API_ETCD_ENDPOINT":  os.Getenv("TEMPLATES_API_ETCD_ENDPOINT"),
			"TEMPLATES_API_ETCD_USERNAME":  os.Getenv("TEMPLATES_API_ETCD_USERNAME"),
			"TEMPLATES_API_ETCD_PASSWORD":  os.Getenv("TEMPLATES_API_ETCD_PASSWORD"),
		}

		return addArgs, addEnvs
	}

	r.ForEachTest(func(test *runner.Test) {
		test.Run(
			runner.WithInitProjectState(),
			runner.WithRunAPIServerAndRequests(
				binaryPath,
				setupAPIServerFn,
				func(s string) string { return s },
			),
			runner.WithAssertProjectState(),
		)
	})
}
