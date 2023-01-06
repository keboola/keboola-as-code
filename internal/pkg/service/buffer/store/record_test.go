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

	time1, err := time.Parse(time.RFC3339, `2006-01-01T15:04:05+07:00`)
	assert.NoError(t, err)
	time2, err := time.Parse(time.RFC3339, `2006-01-02T15:04:05+07:00`)
	assert.NoError(t, err)
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export"}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(time1)}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(time2)}

	record := key.NewRecordKey(sliceKey, time2)
	err = store.CreateRecord(ctx, record, `one,two,"th""ree"`)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
record/00001000/my-receiver/my-export/2006-01-02T08:04:05.000Z/2006-01-02T08:04:05.000Z_%c%c%c%c%c
-----
one,two,"th""ree"
>>>>>
`)
}
