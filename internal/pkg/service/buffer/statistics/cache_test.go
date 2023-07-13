package statistics_test

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestCacheNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	etcdCredentials := etcdhelper.TmpNamespace(t)

	// Create nodes
	clk := clock.NewMock()
	clk.Set(time.Time{})
	clk.Add(time.Second)
	opts := []dependencies.MockedOption{dependencies.WithClock(clk), dependencies.WithEtcdCredentials(etcdCredentials)}
	apiCfg := config.NewAPIConfig()
	workerCfg := config.NewWorkerConfig()
	apiScp1, _ := bufferDependencies.NewMockedAPIScope(t, apiCfg, append(opts, dependencies.WithUniqueID("api-node-1"))...)
	apiScp2, _ := bufferDependencies.NewMockedAPIScope(t, apiCfg, append(opts, dependencies.WithUniqueID("api-node-2"))...)
	workerScp, _ := bufferDependencies.NewMockedWorkerScope(t, workerCfg, append(opts, dependencies.WithUniqueID("worker-node"))...)

	// Create nodes
	collector1 := statistics.NewCollector(apiScp1)
	collector2 := statistics.NewCollector(apiScp2)
	cache, err := statistics.NewCache(workerScp)
	assert.NoError(t, err)

	// Resources
	projectID := keboola.ProjectID(123)
	receiverKey := key.ReceiverKey{ProjectID: projectID, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	slice1Key := key.SliceKey{SliceID: key.SliceID(clk.Now()), FileKey: fileKey}
	slice2Key := key.SliceKey{SliceID: key.SliceID(clk.Now().Add(time.Second)), FileKey: fileKey}
	tableID := keboola.MustParseTableID("in.c-bucket.table")
	columns := []column.Column{column.ID{Name: "col1"}, column.Body{Name: "col2"}}
	mappingKey := key.MappingKey{ExportKey: exportKey, RevisionID: 1}
	mapping := model.Mapping{MappingKey: mappingKey, TableID: tableID, Columns: columns}
	slice1 := model.Slice{SliceKey: slice1Key, State: slicestate.Writing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 1}
	slice2 := model.Slice{SliceKey: slice2Key, State: slicestate.Writing, Mapping: mapping, StorageResource: &keboola.FileUploadCredentials{}, Number: 1}

	// Create records for the sliceS1
	str := apiScp1.Store()
	assert.NoError(t, str.CreateSlice(ctx, slice1))
	assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice1Key, clk.Now()), "..."))
	assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice1Key, clk.Now()), "..."))
	collector1.Notify(slice1Key, 10, 11)
	collector2.Notify(slice1Key, 20, 22)
	<-collector1.Sync(ctx)
	<-collector2.Sync(ctx)

	// Test slice in "opened" state
	assert.Eventually(t, func() bool {
		if v := cache.SliceStats(slice1Key); v.Buffered.RecordsCount == 2 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploading: model.Stats{},
				Uploaded:  model.Stats{},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test slice in "closing" state
	assert.NoError(t, str.SetSliceState(ctx, &slice1, slicestate.Closing))
	assert.Eventually(t, func() bool {
		if v := cache.SliceStats(slice1Key); v.Buffered.RecordsCount == 2 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploading: model.Stats{},
				Uploaded:  model.Stats{},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test slice in "uploading" state
	assert.NoError(t, str.CloseSlice(ctx, &slice1))
	assert.Eventually(t, func() bool {
		if v := cache.SliceStats(slice1Key); v.Uploading.RecordsCount == 2 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploading: model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploaded:  model.Stats{},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test slice in "failed" state
	assert.NoError(t, str.MarkSliceUploadFailed(ctx, &slice1))
	assert.Eventually(t, func() bool {
		if v := cache.SliceStats(slice1Key); v.Uploading.RecordsCount == 2 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploading: model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33},
				Uploaded:  model.Stats{},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test slice in "uploaded" state
	slice1.Statistics.FileSize = 44
	slice1.Statistics.FileGZipSize = 4
	assert.NoError(t, str.ScheduleSliceForRetry(ctx, &slice1))
	assert.NoError(t, str.MarkSliceUploaded(ctx, &slice1))
	assert.Eventually(t, func() bool {
		if v := cache.SliceStats(slice1Key); v.Uploaded.RecordsCount == 2 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33, FileSize: 44, FileGZipSize: 4},
				Buffered:  model.Stats{},
				Uploading: model.Stats{},
				Uploaded:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 2, RecordsSize: 30, BodySize: 33, FileSize: 44, FileGZipSize: 4},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Create records for the slice2
	clk.Add(time.Minute)
	assert.NoError(t, str.CreateSlice(ctx, slice2))
	assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, clk.Now()), "..."))
	assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, clk.Now()), "..."))
	assert.NoError(t, str.CreateRecord(ctx, key.NewRecordKey(slice2Key, clk.Now()), "..."))
	collector1.Notify(slice2Key, 100, 110)
	collector2.Notify(slice2Key, 100, 110)
	collector1.Notify(slice2Key, 100, 110)
	<-collector1.Sync(ctx)
	<-collector2.Sync(ctx)

	// Test file stats: uploaded + opened slice
	assert.Eventually(t, func() bool {
		if v := cache.FileStats(fileKey); v.Buffered.RecordsCount == 3 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 44, FileGZipSize: 4},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 3, RecordsSize: 300, BodySize: 330},
				Uploading: model.Stats{},
				Uploaded:  model.Stats{LastRecordAt: utctime.UTCTime(slice1.OpenedAt()), RecordsCount: 2, RecordsSize: 30, BodySize: 33, FileSize: 44, FileGZipSize: 4},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test file stats: uploaded + uploading slice
	assert.NoError(t, str.SetSliceState(ctx, &slice2, slicestate.Closing))
	assert.NoError(t, str.CloseSlice(ctx, &slice2))
	assert.Eventually(t, func() bool {
		if v := cache.FileStats(fileKey); v.Uploading.RecordsCount == 3 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 44, FileGZipSize: 4},
				Buffered:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 3, RecordsSize: 300, BodySize: 330},
				Uploading: model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 3, RecordsSize: 300, BodySize: 330},
				Uploaded:  model.Stats{LastRecordAt: utctime.UTCTime(slice1.OpenedAt()), RecordsCount: 2, RecordsSize: 30, BodySize: 33, FileSize: 44, FileGZipSize: 4},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test file stats: 2x uploaded slice
	slice2.Statistics.FileSize = 1000
	slice2.Statistics.FileGZipSize = 1000
	assert.NoError(t, str.MarkSliceUploaded(ctx, &slice2))
	assert.Eventually(t, func() bool {
		if v := cache.FileStats(fileKey); v.Uploaded.RecordsCount == 5 {
			assert.Equal(t, model.StatsByType{
				Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 1044, FileGZipSize: 1004},
				Buffered:  model.Stats{},
				Uploading: model.Stats{},
				Uploaded:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 1044, FileGZipSize: 1004},
			}, v)
			return true
		}
		return false
	}, time.Second, 100*time.Millisecond)

	// Test export stats
	assert.Equal(t, model.StatsByType{
		Total:     model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 1044, FileGZipSize: 1004},
		Buffered:  model.Stats{},
		Uploading: model.Stats{},
		Uploaded:  model.Stats{LastRecordAt: utctime.UTCTime(clk.Now()), RecordsCount: 5, RecordsSize: 330, BodySize: 363, FileSize: 1044, FileGZipSize: 1004},
	}, cache.ExportStats(fileKey.ExportKey))
}
