//nolint:forbidigo
package api

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

const (
	instanceIDPlaceholder = "<<INSTANCE_ID>>"
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

	r.ForEachTest(func(test *runner.Test) {
		var repositories string
		if test.TestDirFS().Exists("repository") {
			repositories = fmt.Sprintf("keboola|file://%s", filepath.Join(test.TestDirFS().BasePath(), "repository"))
		} else {
			repositories = "keboola|https://github.com/keboola/keboola-as-code-templates.git|main"
		}
		addArgs := []string{fmt.Sprintf("--repositories=%s", repositories)}

		addEnvs := env.FromMap(map[string]string{
			"TEMPLATES_API_DATADOG_ENABLED":  "false",
			"TEMPLATES_API_STORAGE_API_HOST": test.TestProject().StorageAPIHost(),
			"TEMPLATES_API_ETCD_NAMESPACE":   idgenerator.EtcdNamespaceForTest(),
			"TEMPLATES_API_ETCD_ENDPOINT":    os.Getenv("TEMPLATES_API_ETCD_ENDPOINT"),
			"TEMPLATES_API_ETCD_USERNAME":    os.Getenv("TEMPLATES_API_ETCD_USERNAME"),
			"TEMPLATES_API_ETCD_PASSWORD":    os.Getenv("TEMPLATES_API_ETCD_PASSWORD"),
		})

		requestDecoratorFn := func(request *runner.APIRequestDef) {
			// Replace placeholder by instance ID.
			if strings.Contains(request.Path, instanceIDPlaceholder) {
				result := make(map[string]any)
				_, err := test.
					APIClient().
					R().
					SetResult(&result).
					SetHeader("X-StorageApi-Token", test.TestProject().StorageAPIToken().Token).
					Get("/v1/project/default/instances")

				instances := result["instances"].([]any)
				if assert.NoError(t, err) && assert.Equal(t, 1, len(instances)) {
					instanceId := instances[0].(map[string]any)["instanceId"].(string)
					request.Path = strings.ReplaceAll(request.Path, instanceIDPlaceholder, instanceId)
				}
			}
		}

		test.Run(
			runner.WithInitProjectState(),
			runner.WithRunAPIServerAndRequests(
				binaryPath,
				addArgs,
				addEnvs,
				requestDecoratorFn,
			),
			runner.WithAssertProjectState(),
		)
	})
}
