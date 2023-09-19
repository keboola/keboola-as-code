package statistics_test

import (
	"context"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

type testCase struct {
	Description string
	Prepare     func()
	Records     []expectedEtcdRecord
	Assert      func(statistics.Provider)
}

type expectedEtcdRecord struct {
	Key   string
	Value statistics.Value
}

func (tc *testCase) ExpectedKVs() (out []etcdhelper.KV) {
	for _, record := range tc.Records {
		out = append(out, etcdhelper.KV{Key: record.Key, Value: json.MustEncodeString(record.Value, true)})
	}
	return out
}

// TestStatistics tests the statistics calculation during transitions of a slice and a file through all possible states.
// Two API nodes, two statistics collectors, collect information about new records.
// Then AtomicProvider, L1CacheProvider and L2CacheProvider are used to calculate statistics.
func TestStatistics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	etcdCredentials := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCredentials)

	// Create nodes
	opts := []dependencies.MockedOption{dependencies.WithEtcdCredentials(etcdCredentials)}
	apiCfg := config.NewAPIConfig()
	apiCfg.StatisticsSyncInterval = 50 * time.Millisecond
	workerCfg := config.NewWorkerConfig()
	workerCfg.StatisticsL2CacheTTL = time.Second
	apiScp1, _ := bufferDependencies.NewMockedAPIScope(t, apiCfg, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	apiScp2, _ := bufferDependencies.NewMockedAPIScope(t, apiCfg, append(opts, dependencies.WithUniqueID("api-node-2"))...)
	workerScp, _ := bufferDependencies.NewMockedWorkerScope(t, workerCfg, append(opts, dependencies.WithUniqueID("worker-node"))...)

	// Create collectors
	collector1 := statistics.NewCollector(apiScp1)
	collector2 := statistics.NewCollector(apiScp2)

	// Create providers
	repository := workerScp.StatisticsRepository()
	atomicProvider := repository.AtomicProvider()
	l1Cache := workerScp.StatisticsL1Cache()
	l2Cache := workerScp.StatisticsL2Cache()

	// Resources
	str := apiScp1.Store()
	now, _ := time.Parse(time.RFC3339, "2000-01-01T01:00:00+00:00")
	projectID := keboola.ProjectID(123)
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(now)}
	slice1Key := key.SliceKey{SliceID: key.SliceID(now), FileKey: fileKey}
	slice2Key := key.SliceKey{SliceID: key.SliceID(now.Add(time.Hour)), FileKey: fileKey}
	tableID := keboola.MustParseTableID("in.c-bucket.table")
	columns := []column.Column{column.ID{Name: "col1"}, column.Body{Name: "col2"}}
	mappingKey := key.MappingKey{ExportKey: exportKey, RevisionID: 1}
	mapping := model.Mapping{MappingKey: mappingKey, TableID: tableID, Columns: columns}
	file := model.File{FileKey: fileKey, State: filestate.Opened, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}}
	slice1 := model.Slice{SliceKey: slice1Key, State: slicestate.Writing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 1}
	slice2 := model.Slice{SliceKey: slice2Key, State: slicestate.Writing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 1}
	record1At := utctime.From(slice1.OpenedAt().Add(1 * time.Minute))
	record2At := utctime.From(slice1.OpenedAt().Add(2 * time.Minute))
	record3At := utctime.From(slice2.OpenedAt().Add(3 * time.Minute))
	record4At := utctime.From(slice2.OpenedAt().Add(4 * time.Minute))
	record5At := utctime.From(slice2.OpenedAt().Add(5 * time.Minute))
	assert.NoError(t, str.CreateFile(ctx, file))
	assert.NoError(t, str.CreateSlice(ctx, slice1))

	// Define test cases
	// Slice1 transitions: Opened -> Closing -> Uploading -> Failed -> Uploading -> Uploaded -> Imported
	// Slice2 transitions: Opened -> CLosing -> Uploading -> Uploaded -> Imported
	// File transitions: Opened -> Closing -> Importing -> Failed -> Importing -> Imported
	cases := []testCase{
		{
			Description: "Add records to slice1, statistics are stored per API node",
			Prepare: func() {
				// Add records
				assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice1Key, record1At.Time()), "..."))
				assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice1Key, record2At.Time()), "..."))

				// Trigger stats update and wait for sync to etcd
				etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
					collector1.Notify(record1At.Time(), slice1Key, 10, 11)
				})
				etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
					collector2.Notify(record2At.Time(), slice1Key, 20, 22)
				})
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record1At, RecordsCount: 1, RecordsSize: 10, BodySize: 11},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record2At, LastRecordAt: record2At, RecordsCount: 1, RecordsSize: 20, BodySize: 22},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				}, stats)
			},
		},
		{
			Description: "Switch slice1 from Opened to Closing state",
			Prepare: func() {
				assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Closing))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record1At, RecordsCount: 1, RecordsSize: 10, BodySize: 11},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record2At, LastRecordAt: record2At, RecordsCount: 1, RecordsSize: 20, BodySize: 22},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				}, stats)
			},
		},
		{
			Description: "Switch slice1 from Closing to Uploading state",
			Prepare: func() {
				assert.NoError(t, str.CloseSlice(ctx, &slice1))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record1At, RecordsCount: 1, RecordsSize: 10, BodySize: 11},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record2At, LastRecordAt: record2At, RecordsCount: 1, RecordsSize: 20, BodySize: 22},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				}, stats)
			},
		},
		{
			Description: "Switch slice1 from Uploading to Failed state",
			Prepare: func() {
				assert.NoError(t, str.MarkSliceUploadFailed(ctx, &slice1))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record1At, RecordsCount: 1, RecordsSize: 10, BodySize: 11},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record2At, LastRecordAt: record2At, RecordsCount: 1, RecordsSize: 20, BodySize: 22},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				}, stats)
			},
		},
		{
			Description: "Switch slice1 from Failed to Uploading and then to Uploaded state, per node values are merged and moved to buffered category",
			Prepare: func() {
				assert.NoError(t, str.ScheduleSliceForRetry(ctx, &slice1))
				assert.NoError(t, str.MarkSliceUploaded(ctx, &slice1, statistics.AfterUpload{RecordsCount: 2, FileSize: 44, FileGZipSize: 4}))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				}, stats)
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Description: "Add records to slice2, statistics are stored per API node",
			Prepare: func() {
				// Add records
				assert.NoError(t, str.CreateSlice(ctx, slice2))
				assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, now), "..."))
				assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, now), "..."))
				assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, now), "..."))

				// Wait for sync to etcd
				etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
					collector1.Notify(record3At.Time(), slice2Key, 100, 110)
				})
				etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
					collector2.Notify(record4At.Time(), slice2Key, 100, 110)
				})
				etcdhelper.ExpectModificationInPrefix(t, client, "stats/", func() {
					collector1.Notify(record5At.Time(), slice2Key, 100, 110)
				})
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 2, RecordsSize: 200, BodySize: 220},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record4At, LastRecordAt: record4At, RecordsCount: 1, RecordsSize: 100, BodySize: 110},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330},
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 44, CompressedSize: 4},
				}, stats)
			},
		},
		{
			Description: "Switch slice2 from Opened to Closing state",
			Prepare: func() {
				assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Closing))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 2, RecordsSize: 200, BodySize: 220},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record4At, LastRecordAt: record4At, RecordsCount: 1, RecordsSize: 100, BodySize: 110},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330},
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 44, CompressedSize: 4},
				}, stats)
			},
		},
		{
			Description: "Switch slice2 from Closing to Uploading state",
			Prepare: func() {
				assert.NoError(t, str.CloseSlice(ctx, &slice2))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-1",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 2, RecordsSize: 200, BodySize: 220},
				},
				{
					Key:   "stats/buffered/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/api-node-2",
					Value: statistics.Value{FirstRecordAt: record4At, LastRecordAt: record4At, RecordsCount: 1, RecordsSize: 100, BodySize: 110},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Buffered: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330},
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 44, CompressedSize: 4},
				}, stats)
			},
		},
		{
			Description: "Switch slice2 from Uploading to Uploaded state",
			Prepare: func() {
				assert.NoError(t, str.MarkSliceUploaded(ctx, &slice2, statistics.AfterUpload{RecordsCount: 3, FileSize: 1000, FileGZipSize: 1000}))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		{
			Description: "Switch file from Opened to Closing state",
			Prepare: func() {
				_, err := str.SetFileState(ctx, now, &file, filestate.Closing)
				assert.NoError(t, err)
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		{
			Description: "Switch file from Closing to Importing state",
			Prepare: func() {
				assert.NoError(t, str.CloseFile(ctx, &file))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		{
			Description: "Switch file from Importing to Failed state",
			Prepare: func() {
				assert.NoError(t, str.MarkFileImportFailed(ctx, &file))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/uploaded/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Uploaded: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		{
			Description: "Switch file from Failed to Importing and then to Imported state",
			Prepare: func() {
				assert.NoError(t, str.ScheduleFileForRetry(ctx, &file))
				assert.NoError(t, str.MarkFileImported(ctx, &file))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/imported/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/imported/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Description: "Check all levels of statistics",
			Prepare: func() {
				// nop
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/imported/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T01:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				},
				{
					Key:   "stats/imported/123/my-receiver/my-export/2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z/_nodes_sum",
					Value: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				},
			},
			Assert: func(provider statistics.Provider) {
				// Slice 1
				stats, err := provider.SliceStats(ctx, slice1Key)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record2At, RecordsCount: 2, RecordsSize: 30, BodySize: 33, UncompressedSize: 44, CompressedSize: 4},
				}, stats)
				// Slice 2
				stats, err = provider.SliceStats(ctx, slice2Key)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
					Total:    statistics.Value{FirstRecordAt: record3At, LastRecordAt: record5At, RecordsCount: 3, RecordsSize: 300, BodySize: 330, UncompressedSize: 1000, CompressedSize: 1000},
				}, stats)
				// File
				stats, err = provider.FileStats(ctx, fileKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
				// Export
				stats, err = provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
				// Receiver
				stats, err = provider.ReceiverStats(ctx, receiverKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
		// -------------------------------------------------------------------------------------------------------------
		{
			Description: "Rollup imported statistics on file cleanup and save the sum to the export prefix",
			Prepare: func() {
				assert.NoError(t, repository.RollupImportedOnCleanupOp(fileKey).Do(ctx, client))
			},
			Records: []expectedEtcdRecord{
				{
					Key:   "stats/imported/123/my-receiver/my-export/_cleanup_sum",
					Value: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				},
			},
			Assert: func(provider statistics.Provider) {
				// Export
				stats, err := provider.ExportStats(ctx, exportKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
				// Receiver
				stats, err = provider.ReceiverStats(ctx, receiverKey)
				assert.NoError(t, err)
				assert.Equal(t, statistics.Aggregated{
					Imported: statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
					Total:    statistics.Value{FirstRecordAt: record1At, LastRecordAt: record5At, RecordsCount: 5, RecordsSize: 330, BodySize: 363, UncompressedSize: 1044, CompressedSize: 1004},
				}, stats)
			},
		},
	}

	// Run test cases
	for _, tc := range cases {
		t.Logf(`Test case "%s"`, tc.Description)

		// Make a modification
		tc.Prepare()

		// Check etcd state
		etcdhelper.AssertKVs(t, client, tc.ExpectedKVs(), etcdhelper.WithIgnoredKeyPattern(`^(runtime|file|slice|record)`))

		// Test realtime provider
		tc.Assert(atomicProvider)

		// Get revision of the latest modification
		res, err := client.Get(
			ctx,
			"stats/",
			clientv3.WithPrefix(),
			clientv3.WithLimit(1),
			clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortDescend),
		)
		require.NoError(t, err)
		require.Len(t, res.Kvs, 1)
		expectedRevision := res.Kvs[0].ModRevision

		// Wait for cache sync
		assert.Eventually(t, func() bool {
			return l1Cache.Revision() == expectedRevision
		}, time.Second, 100*time.Millisecond)

		// Test cached L1 provider
		tc.Assert(l1Cache)

		// Test cached L2 provider twice, cold and warm read
		l2Cache.ClearCache()
		tc.Assert(l2Cache)
		tc.Assert(l2Cache)
	}
}
