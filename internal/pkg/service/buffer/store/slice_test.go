package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateSlice(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	slice := sliceForTest()

	_, err := store.createSliceOp(ctx, slice).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
slice/opened/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "opened",
  "sliceNumber": 1
}
>>>>>
`)
}

func TestStore_GetSliceOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	slice := sliceForTest()
	_, err := store.createSliceOp(ctx, slice).Do(ctx, store.client)

	assert.NoError(t, err)

	kv, err := store.getSliceOp(ctx, slice.SliceKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, slice, kv.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
slice/opened/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "opened",
  "sliceNumber": 1
}
>>>>>
`)
}

func TestStore_SetSliceState_Transitions(t *testing.T) {
	t.Parallel()

	// Test all transitions
	testCases := []struct{ from, to slicestate.State }{
		{slicestate.Opened, slicestate.Closing},
		{slicestate.Closing, slicestate.Closed},
		{slicestate.Closed, slicestate.Uploading},
		{slicestate.Uploading, slicestate.Failed},
		{slicestate.Failed, slicestate.Uploading},
		{slicestate.Uploading, slicestate.Uploaded},
	}

	ctx := context.Background()
	store := newStoreForTest(t)
	slice := sliceForTest()
	now, _ := time.Parse(time.RFC3339, "2010-01-01T01:01:01+07:00")

	// Create slice
	assert.NoError(t, store.CreateSlice(ctx, slice))

	for _, tc := range testCases {
		// Trigger transition
		ok, err := store.SetSliceState(ctx, &slice, tc.to, now)
		desc := fmt.Sprintf("%s -> %s", tc.from, tc.to)
		assert.NoError(t, err, desc)
		assert.True(t, ok, desc)
		assert.Equal(t, tc.to, slice.State, desc)
		expected := `
<<<<<
slice/<STATE>/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
%A
  "state": "<STATE>",%A
  "<STATE>At": "2010-01-01T01:01:01+07:00"%A
>>>>>
`
		etcdhelper.AssertKVs(t, store.client, strings.ReplaceAll(expected, "<STATE>", tc.to.String()))

		// Test duplicated transition -> nop
		slice.State = tc.from
		ok, err = store.SetSliceState(ctx, &slice, tc.to, time.Now())
		assert.NoError(t, err, desc)
		assert.False(t, ok, desc)
		assert.Equal(t, tc.to, slice.State, desc)
	}
}

func TestStore_ListUploadedSlices(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T08:04:05.000Z")
	time3, _ := time.Parse(time.RFC3339, "2006-01-03T08:04:05.000Z")
	fileKey := key.FileKey{FileID: key.FileID(time1), ExportKey: exportKey}
	input := []model.Slice{
		{
			SliceKey: key.SliceKey{SliceID: key.SliceID(time2), FileKey: fileKey},
			State:    slicestate.Uploaded,
			Number:   1,
		},
		{
			SliceKey: key.SliceKey{SliceID: key.SliceID(time3), FileKey: fileKey},
			State:    slicestate.Uploaded,
			Number:   2,
		},
	}
	// Create uploaded slices
	for _, slice := range input {
		_, err := store.schema.Slices().Uploaded().ByKey(slice.SliceKey).PutIfNotExists(slice).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	slices, err := store.ListUploadedSlices(ctx, fileKey)
	assert.NoError(t, err)
	assert.Equal(t, input, slices)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
slice/uploaded/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-02T08:04:05.000Z",
  "state": "uploaded",
  "sliceNumber": 1
}
>>>>>

<<<<<
slice/uploaded/00001000/my-receiver/my-export/2006-01-01T08:04:05.000Z/2006-01-03T08:04:05.000Z
-----
{
  "projectId": 1000,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "2006-01-01T08:04:05.000Z",
  "sliceId": "2006-01-03T08:04:05.000Z",
  "state": "uploaded",
  "sliceNumber": 2
}
>>>>>
`)
}

func sliceForTest() model.Slice {
	time1, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	time2, _ := time.Parse(time.RFC3339, "2006-01-02T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{FileID: key.FileID(time1.UTC()), ExportKey: exportKey}
	sliceKey := key.SliceKey{SliceID: key.SliceID(time2.UTC()), FileKey: fileKey}
	return model.Slice{
		SliceKey: sliceKey,
		State:    slicestate.Opened,
		Number:   1,
	}
}
