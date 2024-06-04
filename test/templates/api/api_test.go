//nolint:forbidigo
package api

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
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

	binaryPath := testhelper.CompileBinary(t, "templates-api", "build-templates-api")
	ctx := context.Background()

	runner.
		NewRunner(t).
		ForEachTest(func(test *runner.Test) {
			var repositories string
			if test.TestDirFS().Exists(ctx, "repository") {
				repositories = fmt.Sprintf("keboola|file://%s", filepath.Join(test.TestDirFS().BasePath(), "repository"))
			} else {
				repositories = "keboola|https://github.com/keboola/keboola-as-code-templates.git|main"
			}
			addArgs := []string{fmt.Sprintf("--repositories=%s", repositories)}

			// Connect to the etcd
			etcdCfg := etcdhelper.TmpNamespaceFromEnv(t, "TEMPLATES_ETCD_")
			etcdClient := etcdhelper.ClientForTest(t, etcdCfg)

			addEnvs := env.FromMap(map[string]string{
				"TEMPLATES_DATADOG_ENABLED":  "false",
				"TEMPLATES_NODE_ID":          "test-node",
				"TEMPLATES_STORAGE_API_HOST": test.TestProject().StorageAPIHost(),
				"TEMPLATES_ETCD_NAMESPACE":   etcdCfg.Namespace,
				"TEMPLATES_ETCD_ENDPOINT":    etcdCfg.Endpoint,
				"TEMPLATES_ETCD_USERNAME":    etcdCfg.Username,
				"TEMPLATES_ETCD_PASSWORD":    etcdCfg.Password,
				"TEMPLATES_API_PUBLIC_URL":   "https://templates.keboola.local",
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
					require.NoError(t, err)
					if assert.Len(t, instances, 1) {
						instanceId := instances[0].(map[string]any)["instanceId"].(string)
						request.Path = strings.ReplaceAll(request.Path, instanceIDPlaceholder, instanceId)
					}
				}
			}

			// Run the test
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

			// Write current etcd KVs
			etcdDump, err := etcdhelper.DumpAllToString(ctx, etcdClient)
			require.NoError(test.T(), err)
			require.NoError(test.T(), test.WorkingDirFS().WriteFile(ctx, filesystem.NewRawFile("actual-etcd-kvs.txt", etcdDump)))

			// Assert current etcd state against expected state.
			expectedEtcdKVsPath := "expected-etcd-kvs.txt"
			if test.TestDirFS().IsFile(ctx, expectedEtcdKVsPath) {
				// Compare expected and actual kvs
				etcdhelper.AssertKVsString(test.T(), etcdClient, test.ReadFileFromTestDir(expectedEtcdKVsPath))
			}
		})
}
