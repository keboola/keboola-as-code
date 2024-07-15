//nolint:forbidigo
package api

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

// TestStreamApiE2E runs one Stream API functional test per each subdirectory.
func TestStreamApiE2E(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	binaryPath := testhelper.CompileBinary(t, "stream-service", "build-stream-service")
	ctx := context.Background()

	runner.
		NewRunner(t).
		ForEachTest(func(test *runner.Test) {
			// Get default branch
			defaultBranch, err := test.TestProject().DefaultBranch()
			require.NoError(t, err)
			test.TestProject().Env().Set(`TEST_DEFAULT_BRANCH_ID`, defaultBranch.ID.String())

			// Connect to the etcd
			etcdCfg := etcdhelper.TmpNamespaceFromEnv(t, "STREAM_ETCD_")
			etcdClient := etcdhelper.ClientForTest(t, etcdCfg)

			// Init etcd state
			etcdStateFile := "initial-etcd-kvs.txt"
			if test.TestDirFS().IsFile(ctx, etcdStateFile) {
				etcdStateFileContentStr := test.ReadFileFromTestDir(etcdStateFile)
				err := etcdhelper.PutAllFromSnapshot(context.Background(), etcdClient, etcdStateFileContentStr)
				assert.NoError(test.T(), err)
			}

			// Simulate an empty volume with fixed volumeID
			volumesPath := t.TempDir()
			volumePath := filepath.Join(volumesPath, "hdd", "my-volume")
			require.NoError(t, os.MkdirAll(volumePath, 0o700))
			require.NoError(t, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte("my-volume"), 0o600))

			addEnvs := env.FromMap(map[string]string{
				"STREAM_DATADOG_ENABLED":                           "false",
				"STREAM_NODE_ID":                                   "test-node",
				"STREAM_HOSTNAME":                                  "test-node.localhost",
				"STREAM_STORAGE_API_HOST":                          test.TestProject().StorageAPIHost(),
				"STREAM_STORAGE_VOLUMES_PATH":                      volumesPath,
				"STREAM_STORAGE_LEVEL_LOCAL_WRITER_NETWORK_LISTEN": fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(t)),
				"STREAM_API_PUBLIC_URL":                            "https://stream.keboola.local",
				"STREAM_SOURCE_HTTP_PUBLIC_URL":                    "https://stream-in.keboola.local",
				"STREAM_ETCD_NAMESPACE":                            etcdCfg.Namespace,
				"STREAM_ETCD_ENDPOINT":                             etcdCfg.Endpoint,
				"STREAM_ETCD_USERNAME":                             etcdCfg.Username,
				"STREAM_ETCD_PASSWORD":                             etcdCfg.Password,
			})

			// Run the test
			test.Run(
				runner.WithInitProjectState(),
				runner.WithRunAPIServerAndRequests(
					binaryPath,
					[]string{"api", "storage-writer"}, // start api component and storage-writer to detect the volume
					addEnvs,
					nil,
				),
				runner.WithAssertProjectState(),
			)

			// Write current etcd KVs
			etcdDump, err := etcdhelper.DumpAllToString(ctx, etcdClient)
			assert.NoError(test.T(), err)
			assert.NoError(test.T(), test.WorkingDirFS().WriteFile(ctx, filesystem.NewRawFile("actual-etcd-kvs.txt", etcdDump)))

			// Assert current etcd state against expected state.
			expectedEtcdKVsPath := "expected-etcd-kvs.txt"
			if test.TestDirFS().IsFile(ctx, expectedEtcdKVsPath) {
				// Compare expected and actual kvs
				etcdhelper.AssertKVsString(test.T(), etcdClient, test.ReadFileFromTestDir(expectedEtcdKVsPath))
			}
		})
}
