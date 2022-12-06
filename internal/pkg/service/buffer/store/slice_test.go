package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateSlice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	time1, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: time1, ExportKey: exportKey}
	sliceKey := key.SliceKey{SliceID: time2, FileKey: fileKey}
	slice := model.Slice{
		SliceKey:    sliceKey,
		SliceNumber: 1,
	}
	_, err := store.createSliceOp(ctx, slice).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
slice/1000/github/github-issues/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "fileId": "2006-01-01T15:04:05+07:00",
  "sliceId": "2006-01-02T15:04:05+07:00",
  "sliceNumber": 1
}
>>>>>
`)
}

func TestStore_GetSliceOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T15:04:05+07:00")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	fileKey := key.FileKey{FileID: time1, ExportKey: exportKey}
	sliceKey := key.SliceKey{SliceID: time2, FileKey: fileKey}
	input := model.Slice{
		SliceKey:    sliceKey,
		SliceNumber: 1,
	}
	_, err := store.createSliceOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	kv, err := store.getSliceOp(ctx, sliceKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, kv.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
slice/1000/github/github-issues/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "fileId": "2006-01-01T15:04:05+07:00",
  "sliceId": "2006-01-02T15:04:05+07:00",
  "sliceNumber": 1
}
>>>>>
`)
}
