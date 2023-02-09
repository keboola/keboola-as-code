//nolint:forbidigo
package api

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/runner"
)

const (
	receiverSecretPlaceholder = "<<RECEIVER_SECRET>>"
)

// TestBufferApiE2E runs one Buffer API functional test per each subdirectory.
func TestBufferApiE2E(t *testing.T) {
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
		"buffer-api",
		"BUFFER_API_BUILD_TARGET_PATH",
		"build-buffer-api",
	)

	r.ForEachTest(func(test *runner.Test) {
		etcdNamespace := idgenerator.EtcdNamespaceForTest()
		etcdEndpoint := os.Getenv("BUFFER_ETCD_ENDPOINT")
		etcdUsername := os.Getenv("BUFFER_ETCD_USERNAME")
		etcdPassword := os.Getenv("BUFFER_ETCD_PASSWORD")

		// Connect to the etcd
		etcdClient := etcdhelper.ClientForTestFrom(
			test.T(),
			etcdEndpoint,
			etcdUsername,
			etcdPassword,
			etcdNamespace,
		)

		// Init etcd state
		etcdStateFile := "initial-etcd-kvs.txt"
		if test.TestDirFS().IsFile(etcdStateFile) {
			etcdStateFileContentStr := test.ReadFileFromTestDir(etcdStateFile)
			err := etcdhelper.PutAllFromSnapshot(context.Background(), etcdClient, etcdStateFileContentStr)
			assert.NoError(test.T(), err)
		}

		addEnvs := env.FromMap(map[string]string{
			"KBC_BUFFER_API_HOST":   "buffer.keboola.local",
			"BUFFER_ETCD_NAMESPACE": etcdNamespace,
			"BUFFER_ETCD_ENDPOINT":  etcdEndpoint,
			"BUFFER_ETCD_USERNAME":  etcdUsername,
			"BUFFER_ETCD_PASSWORD":  etcdPassword,
		})

		requestDecoratorFn := func(request *runner.APIRequest) {
			// Replace placeholder by secret loaded from the etcd.
			if strings.Contains(request.Path, receiverSecretPlaceholder) {
				resp, err := etcdClient.Get(context.Background(), "/config/receiver/", etcd.WithPrefix())
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
			runner.WithAssertEtcdState(),
		)

		// Write current etcd KVs
		etcdDump, err := etcdhelper.DumpAll(context.Background(), etcdClient)
		assert.NoError(test.T(), err)
		assert.NoError(test.T(), test.WorkingDirFS().WriteFile(filesystem.NewRawFile("actual-etcd-kvs.txt", etcdDump)))

		// Assert current etcd state against expected state.
		expectedEtcdKVsPath := "expected-etcd-kvs.txt"
		if test.TestDirFS().IsFile(expectedEtcdKVsPath) {
			// Read expected state
			expectedContent := test.ReadFileFromTestDir(expectedEtcdKVsPath)

			// Compare expected and actual kvs
			wildcards.Assert(
				test.T(),
				expectedContent,
				etcdDump,
				`unexpected etcd state, compare "expected-etcd-kvs.txt" from test and "actual-etcd-kvs.txt" from ".out" dir.`,
			)
		}
	})
}
