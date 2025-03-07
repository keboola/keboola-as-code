package keboola_test

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mrand "math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/toxiproxy/v2"
	toxiproxyClient "github.com/Shopify/toxiproxy/v2/client"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/encryption"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// To see details run: TEST_VERBOSE=true go test ./test/stream/bridge/... -v.
func TestKeboolaBridgeWorkflow(t *testing.T) { // nolint: paralleltest
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()

	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	// Update configuration to make the cluster testable
	configFn := func(cfg *config.Config) {
		// Setup encryption
		cfg.Encryption.Provider = encryption.ProviderAES
		cfg.Encryption.AES.SecretKey = secretKey
		// Enable metadata cleanup for removing storage jobs
		cfg.Storage.MetadataCleanup.Enabled = true
		// Disable unrelated workers
		cfg.Storage.DiskCleanup.Enabled = false
		cfg.API.Task.CleanupEnabled = false

		// Use deterministic load balancer
		cfg.Storage.Level.Local.Writer.Network.PipelineBalancer = network.RoundRobinBalancerType

		// In the test, we trigger the slice upload via the records count, the other values are intentionally high.
		cfg.Storage.Level.Staging.Upload = stagingConfig.UploadConfig{
			MinInterval: duration.From(1 * time.Second), // minimum
			Trigger: stagingConfig.UploadTrigger{
				Count:    10,
				Size:     1000 * datasize.MB,
				Interval: duration.From(30 * time.Minute),
			},
		}

		// In the test, we trigger the file import only when sink limit is not reached.
		cfg.Sink.Table.Keboola.JobLimit = 1

		// In the test, we trigger the file import via the records count, the other values are intentionally high.
		cfg.Storage.Level.Target.Import = targetConfig.ImportConfig{
			MinInterval: duration.From(30 * time.Second), // minimum
			Trigger: targetConfig.ImportTrigger{
				Count:       30,
				Size:        1000 * datasize.MB,
				Interval:    duration.From(30 * time.Minute),
				SlicesCount: 100,
				Expiration:  duration.From(30 * time.Minute),
			},
		}

		// Cleanup should be perfomed more frequently to remove already finished storage jobs
		cfg.Storage.MetadataCleanup.Interval = 10 * time.Second
	}

	ts := setup(t)
	ts.startNodes(t, ctx, configFn)
	ts.setupSink(t, ctx)
	defer ts.teardown(t, ctx)

	// Check initial state
	ts.checkState(t, ctx, []file{
		{
			state: model.FileWriting,
			volumes: []volume{
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
			},
		},
	})

	// First successful upload
	ts.logSection(t, "testing first upload")
	ts.testSlicesUpload(t, ctx, sliceUpload{
		records: records{
			startID: 1,
			count:   20,
		},
		expectedFiles: []file{
			{
				state: model.FileWriting,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceUploaded, // <<<<<
							model.SliceWriting,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceUploaded, // <<<<<
							model.SliceWriting,
						},
					},
				},
			},
		},
	})

	// First import
	ts.logSection(t, "testing first import")
	ts.testFileImport(t, ctx, fileImport{
		sendRecords: records{
			startID: 21,
			count:   10,
		},
		expectedFileRecords: records{
			startID: 1,
			count:   30,
		},
		expectedTableRecords: records{
			startID: 1,
			count:   30,
		},
		expectedFiles: []file{
			{
				state: model.FileImported, // <<<<<
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileWriting,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
				},
			},
		},
	})

	ts.logSection(t, "testing second upload")
	ts.testSlicesUpload(
		t,
		ctx,
		sliceUpload{
			records: records{
				startID: 31,
				count:   20,
			},
			expectedFiles: []file{
				{
					state: model.FileImported,
					volumes: []volume{
						{
							slices: []model.SliceState{
								model.SliceImported,
								model.SliceImported,
							},
						},
						{
							slices: []model.SliceState{
								model.SliceImported,
								model.SliceImported,
							},
						},
					},
				},
				{
					state: model.FileWriting,
					volumes: []volume{
						{
							slices: []model.SliceState{
								model.SliceUploaded, // <<<<<
								model.SliceWriting,
							},
						},
						{
							slices: []model.SliceState{
								model.SliceUploaded, // <<<<<
								model.SliceWriting,
							},
						},
					},
				},
			},
		},
	)

	ts.logSection(t, "testing second import")
	ts.testFileImport(t, ctx, fileImport{
		sendRecords: records{
			startID: 51,
			count:   10,
		},
		expectedFileRecords: records{
			startID: 31,
			count:   30,
		},
		expectedTableRecords: records{
			startID: 1,
			count:   60,
		},
		expectedFiles: []file{
			{
				state: model.FileImported,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileImported, // <<<<<
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileWriting,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
				},
			},
		},
	})

	// Test simultaneous slice and file rotations
	ts.logSection(t, "testing simultaneous file and slice rotations, both conditions are met")
	ts.logger.Truncate()
	ts.sendRecords(t, ctx, 61, 69)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed file","component":"storage.node.operator.file.rotation"}
{"level":"info","message":"importing file","component":"storage.node.operator.file.import"}
{"level":"info","message":"imported file","component":"storage.node.operator.file.import"}
		`)
	}, 60*time.Second, 100*time.Millisecond)
	ts.checkKeboolaTable(t, ctx, 1, 129)
}

func TestNetworkIssuesKeboolaBridgeWorkflow(t *testing.T) { // nolint: paralleltest
	metrics := toxiproxy.NewMetricsContainer(nil)
	server := toxiproxy.NewServer(metrics, zerolog.New(os.Stderr))
	go server.Listen("localhost:8474")

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()

	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	// Update configuration to make the cluster testable
	configFn := func(cfg *config.Config) {
		// Setup encryption
		cfg.Encryption.Provider = encryption.ProviderAES
		cfg.Encryption.AES.SecretKey = secretKey
		// Enable metadata cleanup for removing storage jobs
		cfg.Storage.MetadataCleanup.Enabled = true
		// Disable unrelated workers
		cfg.Storage.DiskCleanup.Enabled = false
		cfg.API.Task.CleanupEnabled = false

		// Use deterministic load balancer
		cfg.Storage.Level.Local.Writer.Network.PipelineBalancer = network.RoundRobinBalancerType

		// In the test, we trigger the slice upload via the records count, the other values are intentionally high.
		cfg.Storage.Level.Staging.Upload = stagingConfig.UploadConfig{
			MinInterval: duration.From(1 * time.Second), // minimum
			Trigger: stagingConfig.UploadTrigger{
				Count:    10,
				Size:     1000 * datasize.MB,
				Interval: duration.From(30 * time.Minute),
			},
		}

		// In the test, we trigger the file import only when sink limit is not reached.
		cfg.Sink.Table.Keboola.JobLimit = 1

		cfg.Storage.Level.Target.Import = targetConfig.ImportConfig{
			MinInterval: duration.From(30 * time.Second), // minimum
			Trigger: targetConfig.ImportTrigger{
				Count:       30,
				Size:        1000 * datasize.MB,
				Interval:    duration.From(30 * time.Minute),
				SlicesCount: 100,
				Expiration:  duration.From(30 * time.Minute),
			},
		}

		// Cleanup should be perfomed more frequently to remove already finished storage jobs
		cfg.Storage.MetadataCleanup.Interval = 10 * time.Second
	}

	ts := setup(t)
	client := toxiproxyClient.NewClient("localhost:8474")
	proxy, err := client.CreateProxy("source1", ts.sourceURL1[7:len(ts.sourceURL1)-1], ts.sourceURL1[7:])
	require.NoError(t, err)
	proxy.AddToxic("latency_down", "latency", "downstream", 1.0, map[string]interface{}{
		"latency": 1000,
	})
	t.Cleanup(func() {
		proxy.Delete()
		server.Shutdown()
	})

	ts.proxy = proxy
	ts.startNodes(t, ctx, configFn)
	defer ts.teardown(t, ctx)

	ts.sourceURL1 = ts.sourceURL1[:len(ts.sourceURL1)-1]
	ts.sourcePort1 /= 10
	ts.logger.Infof(ctx, "proxyurl:%s, port:%d", ts.sourceURL1, ts.sourcePort1)
	ts.setupSink(t, ctx)
	// Check initial state
	ts.checkState(t, ctx, []file{
		{
			state: model.FileWriting,
			volumes: []volume{
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
			},
		},
	})

	// First upload
	ts.logSection(t, "testing first upload")
	ts.sendRecords(t, ctx, 1, 20)
	if ts.proxy != nil {
		ts.proxy.Disable()
		time.Sleep(100 * time.Millisecond)
		ts.proxy.Enable()
	}

	// First import
	ts.logSection(t, "testing first import")
	ts.testFileImport(t, ctx, fileImport{
		sendRecords: records{
			startID: 21,
			count:   10,
		},
		expectedFileRecords: records{
			startID: 1,
			count:   30,
		},
		expectedTableRecords: records{
			startID: 1,
			count:   30,
		},
		expectedFiles: []file{
			{
				state: model.FileImported, // <<<<<
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileWriting,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
				},
			},
		},
	})

	ts.logSection(t, "testing second upload")
	ts.sendRecords(t, ctx, 31, 20)
	if ts.proxy != nil {
		ts.proxy.Disable()
		time.Sleep(100 * time.Millisecond)
		ts.proxy.Enable()
	}

	ts.logSection(t, "testing second import")
	ts.testFileImport(t, ctx, fileImport{
		sendRecords: records{
			startID: 51,
			count:   10,
		},
		expectedFileRecords: records{
			startID: 31,
			count:   30,
		},
		expectedTableRecords: records{
			startID: 1,
			count:   60,
		},
		expectedFiles: []file{
			{
				state: model.FileImported,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileImported, // <<<<<
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceImported,
							model.SliceImported,
						},
					},
				},
			},
			{
				state: model.FileWriting,
				volumes: []volume{
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
					{
						slices: []model.SliceState{
							model.SliceWriting,
						},
					},
				},
			},
		},
	})

	// Test simultaneous slice and file rotations
	ts.logSection(t, "testing simultaneous file and slice rotations, both conditions are met")
	ts.logger.Truncate()
	ts.sendRecords(t, ctx, 61, 69)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed file","component":"storage.node.operator.file.rotation"}
{"level":"info","message":"importing file","component":"storage.node.operator.file.import"}
{"level":"info","message":"imported file","component":"storage.node.operator.file.import"}
		`)
	}, 60*time.Second, 100*time.Millisecond)
	ts.checkKeboolaTable(t, ctx, 1, 129)
}

func TestKeboolaBridgeCompressionIssues(t *testing.T) { // nolint: paralleltest
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Second)
	defer cancel()

	secretKey := make([]byte, 32)
	_, err := rand.Read(secretKey)
	require.NoError(t, err)

	// Update configuration to make the cluster testable
	configFn := func(cfg *config.Config) {
		// Setup encryption
		cfg.Encryption.Provider = encryption.ProviderAES
		cfg.Encryption.AES.SecretKey = secretKey
		// Enable metadata cleanup for removing storage jobs
		cfg.Storage.MetadataCleanup.Enabled = true
		// Disable unrelated workers
		cfg.Storage.DiskCleanup.Enabled = false
		cfg.API.Task.CleanupEnabled = false

		// Use deterministic load balancer
		cfg.Storage.Level.Local.Writer.Network.PipelineBalancer = network.RoundRobinBalancerType

		// In the test, we trigger the slice upload via the records count, the other values are intentionally high.
		cfg.Storage.Level.Staging.Upload = stagingConfig.UploadConfig{
			MinInterval: duration.From(1 * time.Second), // minimum
			Trigger: stagingConfig.UploadTrigger{
				Count:    10,
				Size:     1000 * datasize.MB,
				Interval: duration.From(30 * time.Minute),
			},
		}

		// In the test, we trigger the file import only when sink limit is not reached.
		cfg.Sink.Table.Keboola.JobLimit = 1

		// In the test, we trigger the file import via the records count, the other values are intentionally high.
		cfg.Storage.Level.Staging.Operator.SliceRotationCheckInterval = duration.From(2 * time.Second)
		cfg.Storage.Level.Staging.Operator.SliceCloseTimeout = duration.From(2 * time.Minute)
		cfg.Storage.Level.Staging.Operator.SliceUploadCheckInterval = duration.From(60 * time.Second)
		cfg.Storage.Level.Staging.Operator.SliceUploadTimeout = duration.From(4 * time.Minute)

		// In the test, we trigger the file import via the records count, the other values are intentionally high.
		cfg.Storage.Level.Target.Import = targetConfig.ImportConfig{
			MinInterval: duration.From(30 * time.Second), // minimum
			Trigger: targetConfig.ImportTrigger{
				Count:       30,
				Size:        1000 * datasize.MB,
				Interval:    duration.From(30 * time.Minute),
				SlicesCount: 100,
				Expiration:  duration.From(30 * time.Minute),
			},
		}

		// Cleanup should be perfomed more frequently to remove already finished storage jobs
		cfg.Storage.MetadataCleanup.Interval = 10 * time.Second
	}

	ts := setup(t)
	ts.startNodes(t, ctx, configFn)
	ts.setupSink(t, ctx)

	// Check initial state
	ts.checkState(t, ctx, []file{
		{
			state: model.FileWriting,
			volumes: []volume{
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
				{
					slices: []model.SliceState{
						model.SliceWriting,
					},
				},
			},
		},
	})

	// Test corrupted slice upload
	ts.logSection(t, "testing corrupted slice upload")
	ts.sendRecords(t, ctx, 1, 18)
	// Corrupt the slice by writing invalid data
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"opened slice pipeline","component":"storage.router"}
		`)
	}, 20*time.Second, 100*time.Millisecond)
	slices, err := ts.coordinatorScp1.StorageRepository().Slice().ListInState(ts.sinkKey, model.SliceWriting).Do(ctx).All()
	require.NoError(t, err)
	path1 := filesystem.Join(
		ts.volumesPath1,
		strings.NewReplacer(":", "-", ".", "-").Replace(slices[0].SliceKey.FileKey.String()),
		strings.NewReplacer(":", "-", ".", "-").Replace(slices[0].SliceKey.SliceID.String()),
		".slice-source1.csv.gz",
	)
	path2 := filesystem.Join(
		ts.volumesPath1,
		strings.NewReplacer(":", "-", ".", "-").Replace(slices[1].SliceKey.FileKey.String()),
		strings.NewReplacer(":", "-", ".", "-").Replace(slices[1].SliceKey.SliceID.String()),
		".slice-source2.csv.gz",
	)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"debug","message":"opened file","sourceNode.id":"source1","component":"storage.node.writer.volumes.volume"}
		`)
	}, 20*time.Second, 10*time.Millisecond)

	// Corrupt the slice by writing invalid data
	require.NoError(t,
		os.WriteFile(path1,
			[]byte("CORRUPTED_DATA"),
			0o640,
		),
	)
	require.NoError(t,
		os.WriteFile(path2,
			[]byte("CORRUPTED_DATA"),
			0o640,
		),
	)
	ts.sendRecords(t, ctx, 19, 2)
	// Expect slice rotation
	ts.logSection(t, "expecting slices rotation (1s+)")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// Slices are uploaded independently, so we have to use multiple asserts
		rotatingMsg := fmt.Sprintf(`{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: %d, threshold: 10","component":"storage.node.operator.slice.rotation"}`, 10)
		ts.logger.AssertJSONMessages(c, fmt.Sprintf("%s\n%s\n", rotatingMsg, rotatingMsg))
	}, 20*time.Second, 10*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotated slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"rotated slice","component":"storage.node.operator.slice.rotation"}
	`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
	`)
	}, 20*time.Second, 10*time.Millisecond)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"error", "message":"check of hidden file \"%s\" failed: cannot create reader for compressed file \"%s\": cannot create parallel gzip reader: gzip: invalid header","component":"storage.node.reader.volumes"}
{"level":"error", "message":"check of hidden file \"%s\" failed: cannot create reader for compressed file \"%s\": cannot create parallel gzip reader: gzip: invalid header","component":"storage.node.reader.volumes"}
{"level":"error", "message":"slice upload failed:\n- check of hidden file \"%s\" failed: cannot create reader for compressed file \"%s\": cannot create parallel gzip reader: gzip: invalid header","component":"storage.node.operator.slice.upload"}
	`)
	}, 60*time.Second, 1*time.Second)

	ts.logSection(t, "teardown")
	nodes := []withProcess{
		ts.apiScp,
		ts.writerScp1,
		ts.writerScp2,
		ts.readerScp1,
		ts.readerScp2,
		ts.coordinatorScp1,
		ts.coordinatorScp2,
		ts.sourceScp1,
		ts.sourceScp2,
	}

	// Shutdown must work always, in random other
	mrand.Shuffle(len(nodes), func(i, j int) {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	})
	ts.shutdown(t, ctx, nodes)
}

func (ts *testState) testSlicesUpload(t *testing.T, ctx context.Context, expectations sliceUpload) {
	t.Helper()
	ts.logger.Truncate()

	// Check records count
	require.Equal(t, 0, expectations.records.count%2, "records count cannot be balanced evenly into 2 slices")

	// Write N records to both slices to trigger slices upload
	ts.sendRecords(t, ctx, expectations.records.startID, expectations.records.count)

	// Expect slice rotation
	ts.logSection(t, "expecting slices rotation (1s+)")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// Slices are uploaded independently, so we have to use multiple asserts
		rotatingMsg := fmt.Sprintf(`{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: %d, threshold: 10","component":"storage.node.operator.slice.rotation"}`, expectations.records.count/2)
		ts.logger.AssertJSONMessages(c, fmt.Sprintf("%s\n%s\n", rotatingMsg, rotatingMsg))
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotated slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"rotated slice","component":"storage.node.operator.slice.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
		`)
	}, 20*time.Second, 10*time.Millisecond)

	// Expect slices upload
	ts.logSection(t, "expecting slices upload")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploaded slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploaded slice","component":"storage.node.operator.slice.upload"}
		`)
	}, 20*time.Second, 10*time.Millisecond)

	// Check file/slices state after the upload
	files := ts.checkState(t, ctx, expectations.expectedFiles)

	// Get uploaded slices from the last file
	var uploadedSlices []model.Slice
	for _, s := range files[len(files)-1].slices {
		if s.State == model.SliceUploaded {
			uploadedSlices = append(uploadedSlices, s)
		}
	}

	// Check statistics
	assert.Len(t, uploadedSlices, 2)
	sliceStats1, err := ts.apiScp.StatisticsRepository().SliceStats(ctx, uploadedSlices[0].SliceKey)
	require.NoError(t, err)
	sliceStats2, err := ts.apiScp.StatisticsRepository().SliceStats(ctx, uploadedSlices[1].SliceKey)
	require.NoError(t, err)
	assert.NotEmpty(t, sliceStats1.Staging.RecordsCount)
	assert.NotEmpty(t, sliceStats2.Staging.RecordsCount)
	assert.Equal(t, expectations.records.count/2, int(sliceStats1.Staging.RecordsCount))
	assert.Equal(t, expectations.records.count/2, int(sliceStats2.Staging.RecordsCount))

	// Check slices manifest in the staging storage
	ts.logSection(t, "checking slices manifest in the staging storage")
	keboolaFiles, err := ts.project.ProjectAPI().ListFilesRequest(ts.branchID).Send(ctx)
	require.NoError(t, err)
	require.Len(t, *keboolaFiles, len(expectations.expectedFiles))
	downloadCred, err := ts.project.ProjectAPI().GetFileWithCredentialsRequest((*keboolaFiles)[len(*keboolaFiles)-1].FileKey).Send(ctx)
	require.NoError(t, err)
	slicesList, err := keboola.DownloadManifest(ctx, downloadCred)
	require.NoError(t, err)
	require.Len(t, slicesList, len(uploadedSlices))

	// Check content of slices in the staging storage
	ts.logSection(t, "checking slices content in the staging storage")
	var allSlicesContent string
	for _, slice := range slicesList {
		rawReader, err := keboola.DownloadSliceReader(ctx, downloadCred, slice)
		require.NoError(t, err)
		gzipReader, err := gzip.NewReader(rawReader)
		require.NoError(t, err)
		sliceContentBytes, err := io.ReadAll(gzipReader)
		require.NoError(t, err)
		sliceContent := string(sliceContentBytes)
		assert.Equal(t, expectations.records.count/2, strings.Count(sliceContent, "\n"))
		allSlicesContent += sliceContent
		require.NoError(t, gzipReader.Close())
		require.NoError(t, rawReader.Close())
	}
	for i := range expectations.records.count {
		assert.Contains(t, allSlicesContent, fmt.Sprintf(`,"foo%d"`, expectations.records.startID+i))
	}
}

func (ts *testState) testFileImport(t *testing.T, ctx context.Context, expectations fileImport) {
	t.Helper()
	ts.logger.Truncate()

	// Send N records to both slices to trigger file import
	ts.sendRecords(t, ctx, expectations.sendRecords.startID, expectations.sendRecords.count)

	// Expect file rotation
	ts.logSection(t, "expecting file rotation (min 30s from the previous)")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotating file, import conditions met: count threshold met, records count: 30, threshold: 30","component":"storage.node.operator.file.rotation"}
{"level":"info","message":"rotated file","component":"storage.node.operator.file.rotation"}
		`)

		var files []model.File
		files, err := ts.coordinatorScp1.StorageRepository().File().ListInState(ts.sinkKey, model.FileWriting).Do(ctx).All()
		require.NoError(t, err)
		for _, file := range files {
			stats, err := ts.coordinatorScp1.StatisticsRepository().FileStats(ctx, file.FileKey)
			if err == nil && stats.Local.RecordsCount == uint64(expectations.sendRecords.count) {
				return
			}
		}
	}, 60*time.Second, 100*time.Millisecond)

	// Expect slices closing, upload and file closing
	ts.logSection(t, "expecting file closing and slices upload")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closing file","component":"storage.node.operator.file.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"closed slice","component":"storage.node.operator.slice.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploaded slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploaded slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closed file","component":"storage.node.operator.file.rotation"}
		`)
	}, 25*time.Second, 100*time.Millisecond)

	// Expect file import
	ts.logSection(t, "expecting file import")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"importing file","component":"storage.node.operator.file.import"}
{"level":"info","message":"imported file","component":"storage.node.operator.file.import"}
		`)
	}, 60*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"deleted \"1\" jobs","deletedJobsCount":1,"component":"storage.metadata.cleanup"}
		`)
	}, 20*time.Second, 100*time.Millisecond)
	// Check file/slices state after the upload
	files := ts.checkState(t, ctx, expectations.expectedFiles)

	// Check statistics
	prevFile := files[len(files)-2]
	fileStats, err := ts.apiScp.StatisticsRepository().FileStats(ctx, prevFile.file.FileKey)
	require.NoError(t, err)
	assert.Equal(t, expectations.expectedFileRecords.count, int(fileStats.Target.RecordsCount))

	// Check Keboola table
	ts.checkKeboolaTable(t, ctx, expectations.expectedTableRecords.startID, expectations.expectedTableRecords.count)
}

func (ts *testState) checkKeboolaTable(t *testing.T, ctx context.Context, start, expectedCount int) {
	t.Helper()

	ts.logSection(t, fmt.Sprintf("checking Keboola table, expected records %d - %d", start, start+expectedCount-1))
	tablePreview, err := ts.project.ProjectAPI().PreviewTableRequest(keboola.TableKey{BranchID: ts.branchID, TableID: ts.tableID}, keboola.WithLimitRows(500)).Send(ctx)
	require.NoError(t, err)

	assert.Equal(t, []string{"datetime", "body"}, tablePreview.Columns)
	assert.Len(t, tablePreview.Rows, expectedCount)

	tablePreviewStr := json.MustEncodeString(tablePreview, true)
	for i := range expectedCount {
		assert.Contains(t, tablePreviewStr, fmt.Sprintf("foo%d", start+i))
	}
}

func (ts *testState) sendRecords(t *testing.T, ctx context.Context, start, n int) {
	t.Helper()
	ts.logSection(t, fmt.Sprintf("sending HTTP records %d - %d", start, start+n-1))
	for i := range n {
		// Distribute requests to store keys evenly on source nodes
		sourceURL := ts.sourceURL1
		if i%2 == 1 {
			sourceURL = ts.sourceURL2
		}

		go func() {
			var err error
			for {
				var files []model.File
				files, err = ts.coordinatorScp1.StorageRepository().File().ListInState(ts.sinkKey, model.FileWriting).Do(ctx).All()
				require.NoError(t, err)
				for _, file := range files {
					stats, err := ts.coordinatorScp1.StatisticsRepository().FileStats(ctx, file.FileKey)
					if err == nil && stats.Local.RecordsCount == uint64(start+n-1) {
						return
					}
				}

				var req *http.Request
				req, err = http.NewRequestWithContext(ctx, http.MethodPost, sourceURL, strings.NewReader(fmt.Sprintf("foo%d", start+i)))
				require.NoError(t, err)
				var resp *http.Response
				resp, err = ts.httpClient.Do(req)
				if err == nil {
					assert.Equal(t, http.StatusOK, resp.StatusCode)
					assert.NoError(t, resp.Body.Close())
					return
				}

				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
}

func (ts *testState) logSection(t *testing.T, section string) {
	t.Helper()
	fmt.Printf("\n\n########## %s\n\n", section) // nolint:forbidigo
}
