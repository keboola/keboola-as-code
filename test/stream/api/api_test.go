//nolint:forbidigo
package api

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

const (
	receiverSecretPlaceholder = "<<RECEIVER_SECRET>>"
)

// TestBufferApiE2E runs one Buffer API functional test per each subdirectory.
func TestBufferApiE2E(t *testing.T) {
	t.Parallel()

	t.Skip("skipping buffer tests until refactoring is complete")

	if runtime.GOOS == "windows" {
		t.Skip("Skipping API E2E tests on Windows")
	}

	binaryPath := testhelper.CompileBinary(t, "buffer-api", "build-buffer-api")
	ctx := context.Background()

	runner.
		NewRunner(t).
		ForEachTest(func(test *runner.Test) {
			// Connect to the etcd
			etcdCfg := etcdhelper.TmpNamespaceFromEnv(t, "BUFFER_API_ETCD_")
			etcdClient := etcdhelper.ClientForTest(t, etcdCfg)

			// Init etcd state
			etcdStateFile := "initial-etcd-kvs.txt"
			if test.TestDirFS().IsFile(ctx, etcdStateFile) {
				etcdStateFileContentStr := test.ReadFileFromTestDir(etcdStateFile)
				err := etcdhelper.PutAllFromSnapshot(context.Background(), etcdClient, etcdStateFileContentStr)
				assert.NoError(test.T(), err)
			}

			addEnvs := env.FromMap(map[string]string{
				"BUFFER_API_DATADOG_ENABLED":  "false",
				"BUFFER_API_STORAGE_API_HOST": "https://" + test.TestProject().StorageAPIHost(),
				"BUFFER_API_PUBLIC_ADDRESS":   "https://buffer.keboola.local",
				"BUFFER_API_ETCD_NAMESPACE":   etcdCfg.Namespace,
				"BUFFER_API_ETCD_ENDPOINT":    etcdCfg.Endpoint,
				"BUFFER_API_ETCD_USERNAME":    etcdCfg.Username,
				"BUFFER_API_ETCD_PASSWORD":    etcdCfg.Password,
			})

			requestDecoratorFn := func(request *runner.APIRequestDef) {
				// Replace placeholder by secret loaded from the etcd.
				if strings.Contains(request.Path, receiverSecretPlaceholder) {
					resp, err := etcdClient.Get(ctx, "config/receiver/", etcd.WithPrefix())
					if assert.NoError(t, err) && assert.Len(t, resp.Kvs, 1) {
						receiver := make(map[string]any)
						json.MustDecode(resp.Kvs[0].Value, &receiver)
						request.Path = strings.ReplaceAll(request.Path, receiverSecretPlaceholder, receiver["secret"].(string))
					}
				}
			}

			// Run the test
			test.Run(
				runner.WithInitProjectState(),
				runner.WithRunAPIServerAndRequests(
					binaryPath,
					[]string{},
					addEnvs,
					requestDecoratorFn,
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
