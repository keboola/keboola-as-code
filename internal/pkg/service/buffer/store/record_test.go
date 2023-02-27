package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	recordKey := recordKeyForTest("2006-01-02T15:04:10+07:00")
	assert.NoError(t, store.CreateRecord(ctx, recordKey, `one,two,"th""ree"`))

	// Check keys
	etcdhelper.AssertKVsString(t, store.client, `
<<<<<
record/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:10.000Z_%c%c%c%c%c
-----
one,two,"th""ree"
>>>>>
`)
}

func TestStore_CountRecords(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	recordKey1 := recordKeyForTest("2006-01-02T15:04:10+07:00")
	recordKey2 := recordKeyForTest("2006-01-02T15:04:20+07:00")
	recordKey3 := recordKeyForTest("2006-01-02T15:04:30+07:00")

	count, err := store.CountRecords(ctx, recordKey1.SliceKey)
	assert.Equal(t, uint64(0), count)
	assert.NoError(t, err)

	assert.NoError(t, store.CreateRecord(ctx, recordKey1, `...`))
	count, err = store.CountRecords(ctx, recordKey1.SliceKey)
	assert.Equal(t, uint64(1), count)
	assert.NoError(t, err)

	assert.NoError(t, store.CreateRecord(ctx, recordKey2, `...`))
	assert.NoError(t, store.CreateRecord(ctx, recordKey3, `...`))
	count, err = store.CountRecords(ctx, recordKey1.SliceKey)
	assert.Equal(t, uint64(3), count)
	assert.NoError(t, err)
}

func recordKeyForTest(now string) key.RecordKey {
	sliceFileTime, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	recordTime, _ := time.Parse(time.RFC3339, now)
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(sliceFileTime)}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(sliceFileTime)}
	return key.NewRecordKey(sliceKey, recordTime)
}
