package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestStore_GetReceivedStatsByFile(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sm := schema.New(validator.New().Validate)
	store := newStoreForTest(t)

	time1 := time.Time{}.UTC().Add(time.Minute)
	time2 := time1.Add(12 * time.Hour)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey1 := key.ExportKey{ExportID: "my-export-1", ReceiverKey: receiverKey}
	exportKey2 := key.ExportKey{ExportID: "my-export-2", ReceiverKey: receiverKey}
	fileF1Key := key.FileKey{FileID: key.FileID(time1), ExportKey: exportKey1}
	fileF2Key := key.FileKey{FileID: key.FileID(time2), ExportKey: exportKey2}
	sliceF1S1Key := key.SliceKey{SliceID: key.SliceID(time1.Add(1 * time.Hour)), FileKey: fileF1Key}
	sliceF1S2Key := key.SliceKey{SliceID: key.SliceID(time1.Add(2 * time.Hour)), FileKey: fileF1Key}
	sliceF2S1Key := key.SliceKey{SliceID: key.SliceID(time2.Add(1 * time.Hour)), FileKey: fileF2Key}

	// Create receiver
	assert.NoError(t, store.CreateReceiver(ctx, model.Receiver{ReceiverBase: model.ReceiverBase{
		ReceiverKey: receiverKey,
		Name:        "My Receiver",
		Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
	}}))

	// No stats
	stats, err := GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileF1Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileF2Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)

	// First sync - node 1
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node1", []model.SliceStats{
		{
			SliceKey: sliceF1S1Key,
			Stats:    model.Stats{RecordsCount: 10, RecordsSize: 100, BodySize: 1000, LastRecordAt: key.UTCTime(time1.Add(1 * time.Hour))},
		},
		{
			SliceKey: sliceF1S2Key,
			Stats:    model.Stats{RecordsCount: 1, RecordsSize: 10, BodySize: 100, LastRecordAt: key.UTCTime(time1.Add(1 * time.Hour))},
		},
		{
			SliceKey: sliceF2S1Key,
			Stats:    model.Stats{RecordsCount: 2, RecordsSize: 20, BodySize: 200, LastRecordAt: key.UTCTime(time2.Add(1 * time.Hour))},
		},
	}))
	// node1[sliceF1S1Key] + node1[sliceF1S2Key]
	assertFileStats(t, store, fileF1Key, 10+1, 100+10, 1000+100, time1.Add(1*time.Hour))
	// node1[sliceF2S1Key]
	assertFileStats(t, store, fileF2Key, 2, 20, 200, time2.Add(1*time.Hour))

	// Update - node 1
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node1", []model.SliceStats{
		{
			SliceKey: sliceF1S1Key,
			Stats:    model.Stats{RecordsCount: 30, RecordsSize: 300, BodySize: 3000, LastRecordAt: key.UTCTime(time1.Add(2 * time.Hour))},
		},
		{
			SliceKey: sliceF1S2Key,
			Stats:    model.Stats{RecordsCount: 4, RecordsSize: 40, BodySize: 400, LastRecordAt: key.UTCTime(time1.Add(2 * time.Hour))},
		},
		{
			SliceKey: sliceF2S1Key,
			Stats:    model.Stats{RecordsCount: 5, RecordsSize: 50, BodySize: 500, LastRecordAt: key.UTCTime(time2.Add(2 * time.Hour))},
		},
	}))
	// node1[sliceF1S1Key] + node1[sliceF1S2Key]
	assertFileStats(t, store, fileF1Key, 30+4, 300+40, 3000+400, time1.Add(2*time.Hour))
	// node1[sliceF2S1Key]
	assertFileStats(t, store, fileF2Key, 5, 50, 500, time2.Add(2*time.Hour))

	// First sync - node 2
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node2", []model.SliceStats{
		{
			SliceKey: sliceF1S1Key,
			Stats:    model.Stats{RecordsCount: 60, RecordsSize: 600, BodySize: 6000, LastRecordAt: key.UTCTime(time1.Add(3 * time.Hour))},
		},
		{
			SliceKey: sliceF1S2Key,
			Stats:    model.Stats{RecordsCount: 7, RecordsSize: 70, BodySize: 700, LastRecordAt: key.UTCTime(time1.Add(3 * time.Hour))},
		},
		{
			SliceKey: sliceF2S1Key,
			Stats:    model.Stats{RecordsCount: 8, RecordsSize: 80, BodySize: 800, LastRecordAt: key.UTCTime(time2.Add(3 * time.Hour))},
		},
	}))
	// node1[sliceF1S1Key] + node1[sliceF1S2Key] + node2[sliceF1S1Key] + node2[sliceF1S2Key]
	assertFileStats(t, store, fileF1Key, 30+4+60+7, 300+40+600+70, 3000+400+6000+700, time1.Add(3*time.Hour))
	// node1[sliceF2S1Key] + node2[sliceF2S1Key]
	assertFileStats(t, store, fileF2Key, 5+8, 50+80, 500+800, time2.Add(3*time.Hour))

	// Update - node 2
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node2", []model.SliceStats{
		{
			SliceKey: sliceF1S1Key,
			Stats:    model.Stats{RecordsCount: 10, RecordsSize: 100, BodySize: 1000, LastRecordAt: key.UTCTime(time1.Add(4 * time.Hour))},
		},
		{
			SliceKey: sliceF1S2Key,
			Stats:    model.Stats{RecordsCount: 2, RecordsSize: 20, BodySize: 200, LastRecordAt: key.UTCTime(time1.Add(4 * time.Hour))},
		},
		{
			SliceKey: sliceF2S1Key,
			Stats:    model.Stats{RecordsCount: 3, RecordsSize: 30, BodySize: 300, LastRecordAt: key.UTCTime(time2.Add(4 * time.Hour))},
		},
	}))
	// node1[sliceF1S1Key] + node1[sliceF1S2Key] + node2[sliceF1S1Key] + node2[sliceF1S2Key]
	assertFileStats(t, store, fileF1Key, 30+4+10+2, 300+40+100+20, 3000+400+1000+200, time1.Add(4*time.Hour))
	// node1[sliceF2S1Key] + node2[sliceF2S1Key]
	assertFileStats(t, store, fileF2Key, 5+3, 50+30, 500+300, time2.Add(4*time.Hour))

	// Delete receiver - no stats
	assert.NoError(t, store.DeleteReceiver(ctx, receiverKey))
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileF1Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileF2Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
}

func TestStore_GetReceivedStatsByFile_Many(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sm := schema.New(validator.New().Validate)
	store := newStoreForTest(t)

	fileOpenedAt := time.Time{}.UTC().Add(time.Minute)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: key.FileID(fileOpenedAt), ExportKey: exportKey}

	// 3 nodes, each sync stats for 1000 slices
	for n := 1; n <= 3; n++ {
		var stats []model.SliceStats
		for s := 1; s <= 1000; s++ {
			sliceOpenedAt := fileOpenedAt.Add(time.Duration(s) * time.Hour)
			stats = append(stats, model.SliceStats{
				SliceKey: key.SliceKey{SliceID: key.SliceID(sliceOpenedAt), FileKey: fileKey},
				Stats: model.Stats{
					RecordsCount: 1,
					RecordsSize:  10,
					LastRecordAt: key.UTCTime(sliceOpenedAt.Add(1 * time.Minute)),
				},
			})
		}
		nodeID := fmt.Sprintf("node-%d", n)
		assert.NoError(t, store.UpdateSliceReceivedStats(ctx, nodeID, stats))
	}

	stats, err := GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileKey).GetAll())
	assert.NoError(t, err)
	assert.Equal(t, uint64(3*1000), stats.RecordsCount)
	assert.Equal(t, datasize.ByteSize(3*10*1000), stats.RecordsSize)
	assert.Equal(t, key.UTCTime(fileOpenedAt.Add(1000*time.Hour+1*time.Minute)), stats.LastRecordAt)
}

func TestStore_GetReceivedStatsBySlice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sm := schema.New(validator.New().Validate)
	store := newStoreForTest(t)

	time1 := time.Time{}.UTC().Add(time.Minute)
	time2 := time1.Add(12 * time.Hour)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey1 := key.ExportKey{ExportID: "my-export-1", ReceiverKey: receiverKey}
	exportKey2 := key.ExportKey{ExportID: "my-export-2", ReceiverKey: receiverKey}
	slice1Key := key.SliceKey{
		SliceID: key.SliceID(time1.Add(time.Hour)),
		FileKey: key.FileKey{FileID: key.FileID(time1), ExportKey: exportKey1},
	}
	slice2Key := key.SliceKey{
		SliceID: key.SliceID(time2.Add(time.Hour)),
		FileKey: key.FileKey{FileID: key.FileID(time2), ExportKey: exportKey2},
	}

	// Create receiver
	assert.NoError(t, store.CreateReceiver(ctx, model.Receiver{ReceiverBase: model.ReceiverBase{
		ReceiverKey: receiverKey,
		Name:        "My Receiver",
		Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
	}}))

	// No stats
	stats, err := GetStatsFrom(ctx, store, sm.ReceivedStats().InSlice(slice1Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InSlice(slice2Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)

	// First sync - node 1
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node1", []model.SliceStats{
		{
			SliceKey: slice1Key,
			Stats:    model.Stats{RecordsCount: 10, RecordsSize: 100, BodySize: 1000, LastRecordAt: key.UTCTime(time1.Add(1 * time.Hour))},
		},
		{
			SliceKey: slice2Key,
			Stats:    model.Stats{RecordsCount: 1, RecordsSize: 10, BodySize: 100, LastRecordAt: key.UTCTime(time2.Add(1 * time.Hour))},
		},
	}))
	assertSliceStats(t, store, slice1Key, 10, 100, 1000, time1.Add(1*time.Hour))
	assertSliceStats(t, store, slice2Key, 1, 10, 100, time2.Add(1*time.Hour))

	// Update - node 1
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node1", []model.SliceStats{
		{
			SliceKey: slice1Key,
			Stats:    model.Stats{RecordsCount: 20, RecordsSize: 200, BodySize: 2000, LastRecordAt: key.UTCTime(time1.Add(2 * time.Hour))},
		},
		{
			SliceKey: slice2Key,
			Stats:    model.Stats{RecordsCount: 2, RecordsSize: 20, BodySize: 200, LastRecordAt: key.UTCTime(time2.Add(2 * time.Hour))},
		},
	}))
	assertSliceStats(t, store, slice1Key, 20, 200, 2000, time1.Add(2*time.Hour))
	assertSliceStats(t, store, slice2Key, 2, 20, 200, time2.Add(2*time.Hour))

	// First sync - node 2
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node2", []model.SliceStats{
		{
			SliceKey: slice1Key,
			Stats:    model.Stats{RecordsCount: 100, RecordsSize: 1000, BodySize: 10000, LastRecordAt: key.UTCTime(time1.Add(3 * time.Hour))},
		},
		{
			SliceKey: slice2Key,
			Stats:    model.Stats{RecordsCount: 10, RecordsSize: 100, BodySize: 1000, LastRecordAt: key.UTCTime(time2.Add(3 * time.Hour))},
		},
	}))
	assertSliceStats(t, store, slice1Key, 20+100, 200+1000, 2000+10000, time1.Add(3*time.Hour))
	assertSliceStats(t, store, slice2Key, 2+10, 20+100, 200+1000, time2.Add(3*time.Hour))

	// Update - node 2
	assert.NoError(t, store.UpdateSliceReceivedStats(ctx, "node2", []model.SliceStats{
		{
			SliceKey: slice1Key,
			Stats:    model.Stats{RecordsCount: 200, RecordsSize: 2000, BodySize: 20000, LastRecordAt: key.UTCTime(time1.Add(4 * time.Hour))},
		},
		{
			SliceKey: slice2Key,
			Stats:    model.Stats{RecordsCount: 20, RecordsSize: 200, BodySize: 2000, LastRecordAt: key.UTCTime(time2.Add(4 * time.Hour))},
		},
	}))
	assertSliceStats(t, store, slice1Key, 20+200, 200+2000, 2000+20000, time1.Add(4*time.Hour))
	assertSliceStats(t, store, slice2Key, 2+20, 20+200, 200+2000, time2.Add(4*time.Hour))

	// Delete receiver - no stats
	assert.NoError(t, store.DeleteReceiver(ctx, receiverKey))
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InSlice(slice1Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InSlice(slice2Key).GetAll())
	assert.NoError(t, err)
	assert.Empty(t, stats)
}

func TestStore_GetReceivedStatsBySlice_Many(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	sm := schema.New(validator.New().Validate)
	store := newStoreForTest(t)

	fileOpenedAt := time.Time{}.UTC().Add(time.Minute)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: key.FileID(fileOpenedAt), ExportKey: exportKey}
	sliceKey := key.SliceKey{SliceID: key.SliceID(fileOpenedAt), FileKey: fileKey}

	// 100 nodes, each with 1 slice
	for n := 1; n <= 100; n++ {
		nodeID := fmt.Sprintf("node-%d", n)
		assert.NoError(t, store.UpdateSliceReceivedStats(ctx, nodeID, []model.SliceStats{
			{
				SliceKey: sliceKey,
				Stats: model.Stats{
					RecordsCount: 1,
					RecordsSize:  10,
					LastRecordAt: key.UTCTime(fileOpenedAt.Add(time.Duration(n) * time.Minute)),
				},
			},
		}))
	}

	// Slice stats
	stats, err := GetStatsFrom(ctx, store, sm.ReceivedStats().InSlice(sliceKey).GetAll())
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), stats.RecordsCount)
	assert.Equal(t, datasize.ByteSize(10*100), stats.RecordsSize)
	assert.Equal(t, key.UTCTime(fileOpenedAt.Add(100*time.Minute)), stats.LastRecordAt)

	// File stats
	stats, err = GetStatsFrom(ctx, store, sm.ReceivedStats().InFile(fileKey).GetAll())
	assert.NoError(t, err)
	assert.Equal(t, uint64(100), stats.RecordsCount)
	assert.Equal(t, datasize.ByteSize(10*100), stats.RecordsSize)
	assert.Equal(t, key.UTCTime(fileOpenedAt.Add(100*time.Minute)), stats.LastRecordAt)
}

func TestStore_UpdateSliceStats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	time1 := time.Time{}.Add(time.Minute)
	time2 := time1.Add(time.Hour * 12)
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey1 := key.FileKey{FileID: key.FileID(time1), ExportKey: exportKey}
	fileKey2 := key.FileKey{FileID: key.FileID(time2), ExportKey: exportKey}

	err := store.UpdateSliceReceivedStats(ctx, "my-node", []model.SliceStats{
		{
			SliceKey: key.SliceKey{SliceID: key.SliceID(time1.Add(time.Hour)), FileKey: fileKey1},
			Stats: model.Stats{
				RecordsCount: 111,
				RecordsSize:  1111,
				BodySize:     11111,
				LastRecordAt: key.UTCTime(time1.Add(time.Hour * 2)),
			},
		},
		{
			SliceKey: key.SliceKey{SliceID: key.SliceID(time2.Add(time.Hour)), FileKey: fileKey2},
			Stats: model.Stats{
				RecordsCount: 222,
				RecordsSize:  2222,
				BodySize:     22222,
				LastRecordAt: key.UTCTime(time2.Add(time.Hour * 2)),
			},
		},
	})
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client.KV, `
<<<<<
stats/received/00000123/my-receiver/my-export/0001-01-01T00:01:00.000Z/0001-01-01T01:01:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T00:01:00.000Z",
  "sliceId": "0001-01-01T01:01:00.000Z",
  "lastRecordAt": "0001-01-01T02:01:00.000Z",
  "recordsCount": 111,
  "recordsSize": "1111B",
  "bodySize": "11111B"
}
>>>>>

<<<<<
stats/received/00000123/my-receiver/my-export/0001-01-01T12:01:00.000Z/0001-01-01T13:01:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T12:01:00.000Z",
  "sliceId": "0001-01-01T13:01:00.000Z",
  "lastRecordAt": "0001-01-01T14:01:00.000Z",
  "recordsCount": 222,
  "recordsSize": "2222B",
  "bodySize": "22222B"
}
>>>>>
`)
}

func assertFileStats(t *testing.T, store *Store, fileKey key.FileKey, recordsCount uint64, recordsSize, bodySize datasize.ByteSize, lastAt time.Time) {
	t.Helper()
	sm := schema.New(validator.New().Validate)
	stats, err := GetStatsFrom(context.Background(), store, sm.ReceivedStats().InFile(fileKey).GetAll())
	assert.NoError(t, err)
	assert.Equal(t, recordsCount, stats.RecordsCount)
	assert.Equal(t, recordsSize, stats.RecordsSize)
	assert.Equal(t, bodySize, stats.BodySize)
	assert.Equal(t, key.UTCTime(lastAt), stats.LastRecordAt)
}

func assertSliceStats(t *testing.T, store *Store, sliceKey key.SliceKey, recordsCount uint64, recordsSize, bodySize datasize.ByteSize, lastAt time.Time) {
	t.Helper()
	sm := schema.New(validator.New().Validate)
	stats, err := GetStatsFrom(context.Background(), store, sm.ReceivedStats().InSlice(sliceKey).GetAll())
	assert.NoError(t, err)
	assert.Equal(t, recordsCount, stats.RecordsCount)
	assert.Equal(t, recordsSize, stats.RecordsSize)
	assert.Equal(t, bodySize, stats.BodySize)
	assert.Equal(t, key.UTCTime(lastAt), stats.LastRecordAt)
}
