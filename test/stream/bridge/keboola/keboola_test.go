package keboola_test

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type withProcess interface {
	Process() *servicectx.Process
}

func TestKeboolaBridgeWorkflow(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Update configuration to make the cluster testable
	configFn := func(cfg *config.Config) {
		// Use deterministic load balancer
		cfg.Storage.Level.Local.Writer.Network.PipelineBalancer = network.RoundRobinBalancerType

		// In the test, we trigger the slice upload via the records count, the other values are intentionally high.
		cfg.Storage.Level.Staging.Upload.Trigger = stagingConfig.UploadTrigger{
			Count:    10,
			Size:     1000 * datasize.MB,
			Interval: duration.From(30 * time.Minute),
		}

		// In the test, we trigger the file import via the records count, the other values are intentionally high.
		cfg.Storage.Level.Target.Import.Trigger = targetConfig.ImportTrigger{
			Count:       30,
			Size:        1000 * datasize.MB,
			Interval:    duration.From(30 * time.Minute),
			SlicesCount: 100,
			Expiration:  duration.From(30 * time.Minute),
		}
	}

	ts := setup(t, ctx, configFn)
	defer ts.teardown(t, ctx)

	ts.testSlicesUpload(
		t,
		ctx,
		0,
		20,
		[]model.File{
			{State: model.FileWriting},
		},
		[]model.Slice{
			{State: model.SliceWriting},
			{State: model.SliceWriting},
		},
		[]model.Slice{
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
		},
	)
	ts.testFileImport(
		t,
		ctx,
		30,
		[]model.File{{State: model.FileWriting}},
		[]model.Slice{
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
		},
	)
	// Trigger new import of file by pushing another 30 slices
	ts.testSlicesUpload(
		t,
		ctx,
		30,
		20,
		[]model.File{
			{State: model.FileImported},
			{State: model.FileWriting},
		},
		[]model.Slice{
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceWriting},
			{State: model.SliceWriting},
		},
		[]model.Slice{
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
		},
	)
	ts.testFileImport(
		t,
		ctx,
		60,
		[]model.File{
			{State: model.FileImported},
			{State: model.FileWriting},
		},
		[]model.Slice{
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceImported},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
			{State: model.SliceUploaded},
			{State: model.SliceWriting},
		},
	)

	// Test simultanous slice and file rotations
	ts.sendRecords(t, ctx, 69)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully closed file","component":"storage.node.operator.file.rotation"}
		`)
	}, 15*time.Second, 100*time.Millisecond)
}

func (ts *testState) testSlicesUpload(
	t *testing.T,
	ctx context.Context,
	startKey,
	count int,
	expectedFiles []model.File,
	expectedSlices []model.Slice,
	expectedUploadedSlices []model.Slice,
) {
	t.Helper()
	// Check file/slices state.
	// There is one opened file (FileWriting), two disk writer nodes, each with one volume, with one opened slice (SliceWriting)
	files := ts.getFiles(t, ctx)
	slices := ts.getSlices(t, ctx)
	assert.Len(t, files, len(expectedFiles))
	assert.Len(t, slices, len(expectedSlices))
	for i, file := range expectedFiles {
		assert.Equal(t, file.State, files[i].State)
	}

	half := len(expectedSlices) / 2
	for i := range half {
		// Volume 1
		assert.Equal(t, expectedSlices[i].State, slices[i].State)
		// Volume 2
		assert.Equal(t, expectedSlices[half+i].State, slices[half+i].State)
	}

	// Write 10 records to both slices to trigger slices upload
	ts.logger.Truncate()
	ts.sendRecords(t, ctx, count)

	// sink.router and storage.router logs have no telemetry/details, for example: opened sink pipeline, opened encoding pipeline .... add info about slice/file...

	// Expect slice rotation
	ts.logSection(t, "expecting slices rotation")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lastSlice := len(expectedSlices)
		// Both slices contain 10 records, deterministic RoundRobinBalancer has been used
		stats1, err := ts.apiScp.StatisticsRepository().SliceStats(ctx, slices[lastSlice-2].SliceKey)
		assert.NoError(c, err)
		if half == 10 {
			assert.Equal(c, uint64(10), stats1.Total.RecordsCount)
		}
		stats2, err := ts.apiScp.StatisticsRepository().SliceStats(ctx, slices[lastSlice-1].SliceKey)
		assert.NoError(c, err)
		if half == 10 {
			assert.Equal(c, uint64(10), stats2.Total.RecordsCount)
		}
	}, 10*time.Second, 10*time.Millisecond)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// Slices are uploaded independently, so we have to use multiple asserts
		if half == 10 {
			ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: 10, threshold: 10","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: 10, threshold: 10","component":"storage.node.operator.slice.rotation"}
		`)
		} else {
			ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: %d, threshold: 10","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"rotating slice, upload conditions met: count threshold met, records count: %d, threshold: 10","component":"storage.node.operator.slice.rotation"}
		`)
		}
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully rotated slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"successfully rotated slice","component":"storage.node.operator.slice.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully closed slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"successfully closed slice","component":"storage.node.operator.slice.rotation"}
		`)
	}, 10*time.Second, 10*time.Millisecond)

	// Expect slices upload
	ts.logSection(t, "expecting slices upload")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully uploaded slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"successfully uploaded slice","component":"storage.node.operator.slice.upload"}
		`)
	}, 15*time.Second, 10*time.Millisecond)

	// Check file/slices state after the upload
	files = ts.getFiles(t, ctx)
	slices = ts.getSlices(t, ctx)
	assert.Len(t, files, len(expectedFiles))
	assert.Len(t, slices, len(expectedUploadedSlices))
	for i, file := range expectedFiles {
		assert.Equal(t, file.State, files[i].State)
	}

	for i := range expectedUploadedSlices {
		assert.Equal(t, expectedUploadedSlices[i].State, slices[i].State)
		assert.False(t, slices[i].LocalStorage.IsEmpty)
	}

	// Check slices manifest in the staging storage
	ts.logSection(t, "checking slices manifest in the staging storage")
	keboolaFiles, err := ts.project.ProjectAPI().ListFilesRequest(ts.branchID).Send(ctx)
	require.NoError(t, err)
	require.Len(t, *keboolaFiles, len(expectedFiles))
	downloadCred, err := ts.project.ProjectAPI().GetFileWithCredentialsRequest((*keboolaFiles)[len(*keboolaFiles)-1].FileKey).Send(ctx)
	require.NoError(t, err)
	slicesList, err := keboola.DownloadManifest(ctx, downloadCred)
	require.NoError(t, err)
	require.Len(t, slicesList, count/10)

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
		if half == 10 {
			assert.Equal(t, count/2, strings.Count(sliceContent, "\n"))
		}
		allSlicesContent += sliceContent
		require.NoError(t, gzipReader.Close())
		require.NoError(t, rawReader.Close())
	}

	for i := range count {
		assert.True(t, strings.Contains(allSlicesContent, fmt.Sprintf(`,"foo%d"`, startKey+i+1)))
	}
}

func (ts *testState) testFileImport(
	t *testing.T,
	ctx context.Context,
	expectedCount int,
	expectedFiles []model.File,
	expectedSlices []model.Slice,
) {
	// Check file/slices state
	// There is one opened file (FileWriting), two disk writer nodes, each with one volume - with one opened slice (SliceWriting)
	// There are also two already uploaded slices.
	t.Helper()
	files := ts.getFiles(t, ctx)
	slices := ts.getSlices(t, ctx)
	assert.Len(t, files, len(expectedFiles))
	assert.Len(t, slices, len(expectedSlices))
	for i, file := range expectedFiles {
		assert.Equal(t, file.State, files[i].State)
	}

	half := len(expectedSlices) / 2
	for i := range half {
		// Volume 1
		assert.Equal(t, expectedSlices[i].State, slices[i].State)
		// Volume 2
		assert.Equal(t, expectedSlices[half+i].State, slices[half+i].State)
	}

	// Write 5 records to both slices to trigger file import
	ts.logger.Truncate()
	ts.sendRecords(t, ctx, 10)

	// Expect file rotation
	ts.logSection(t, "expecting file rotation")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		lastFile := len(expectedFiles)
		stats1, err := ts.apiScp.StatisticsRepository().FileStats(ctx, files[lastFile-1].FileKey)
		assert.NoError(c, err)
		assert.Equal(c, uint64(30), stats1.Staging.RecordsCount)
	}, 10*time.Second, 10*time.Millisecond)
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		if expectedCount > 30 {
			ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotating file, import conditions met: count threshold met, records count: %d, threshold: 30","component":"storage.node.operator.file.rotation"}
{"level":"info","message":"successfully rotated file","component":"storage.node.operator.file.rotation"}
		`)
		} else {
			ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"rotating file, import conditions met: count threshold met, records count: 30, threshold: 30","component":"storage.node.operator.file.rotation"}
{"level":"info","message":"successfully rotated file","component":"storage.node.operator.file.rotation"}
		`)
		}
	}, 10*time.Second, 100*time.Millisecond)

	// Expect slices closing, upload and file closing
	ts.logSection(t, "expecting file closing and slices upload")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"closing file","component":"storage.node.operator.file.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully closed slice","component":"storage.node.operator.slice.rotation"}
{"level":"info","message":"successfully closed slice","component":"storage.node.operator.slice.rotation"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"uploading slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully uploaded slice","component":"storage.node.operator.slice.upload"}
{"level":"info","message":"successfully uploaded slice","component":"storage.node.operator.slice.upload"}
		`)
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"successfully closed file","component":"storage.node.operator.file.rotation"}
		`)
	}, 15*time.Second, 100*time.Millisecond)

	// Expect file import
	ts.logSection(t, "expecting file import")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		ts.logger.AssertJSONMessages(c, `
{"level":"info","message":"importing file","component":"storage.node.operator.file.import"}
{"level":"info","message":"successfully imported file","component":"storage.node.operator.file.import"}
		`)
	}, 60*time.Second, 100*time.Millisecond)

	// Check Keboola table
	ts.logSection(t, "checking Keboola table after the file import")
	tablePreview, err := ts.project.ProjectAPI().PreviewTableRequest(keboola.TableKey{BranchID: ts.branchID, TableID: ts.tableID}, keboola.WithLimitRows(100)).Send(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"datetime", "body"}, tablePreview.Columns)
	assert.Len(t, tablePreview.Rows, expectedCount)
	tablePreviewStr := json.MustEncodeString(tablePreview, true)
	for i := range expectedCount {
		assert.True(t, strings.Contains(tablePreviewStr, fmt.Sprintf("foo%d", i+1)))
	}
}

func (ts *testState) sendRecords(t *testing.T, ctx context.Context, n int) {
	t.Helper()
	ts.logSection(t, fmt.Sprintf("sending %d HTTP records", n))
	for range n {
		ts.recordID++
		// Distribute requests to store keys evenly on source nodes
		sourceURL := ts.sourceURL1
		if ts.recordID%2 == 1 {
			sourceURL = ts.sourceURL2
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, sourceURL, strings.NewReader(fmt.Sprintf("foo%d", ts.recordID)))
		require.NoError(t, err)
		resp, err := ts.httpClient.Do(req)
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.NoError(t, resp.Body.Close())
		}
	}
}

func (ts *testState) getSlices(t *testing.T, ctx context.Context) []model.Slice {
	t.Helper()
	slices, err := ts.apiScp.StorageRepository().Slice().ListIn(ts.sinkKey).Do(ctx).All()
	require.NoError(t, err)
	return slices
}

func (ts *testState) getFiles(t *testing.T, ctx context.Context) []model.File {
	t.Helper()
	files, err := ts.apiScp.StorageRepository().File().ListIn(ts.sinkKey).Do(ctx).All()
	require.NoError(t, err)
	return files
}

func (ts *testState) logSection(t *testing.T, section string) {
	t.Helper()
	fmt.Printf("\n\n########## %s\n\n", section) // nolint:forbidigo
}

func formatHTTPSourceURL(t *testing.T, baseURL string, entity definition.Source) string {
	t.Helper()
	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	return u.
		JoinPath("stream", entity.ProjectID.String(), entity.SourceID.String(), entity.HTTP.Secret).
		String()
}
