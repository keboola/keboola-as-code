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

func TestStore_UpdateSliceStats(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	time0 := time.Time{}.Add(time.Minute)
	time1 := time0.Add(time.Hour * 12)
	err := store.UpdateSliceStats(ctx, []model.SliceStats{
		{
			SliceStatsKey:  key.NewSliceStatsKey(123, "my-receiver", "my-export", time0, time0.Add(time.Hour), "my-node"),
			Count:          111,
			Size:           1111,
			LastReceivedAt: time0.Add(time.Hour * 2),
		},
		{
			SliceStatsKey:  key.NewSliceStatsKey(123, "my-receiver", "my-export", time1, time1.Add(time.Hour), "my-node"),
			Count:          222,
			Size:           2222,
			LastReceivedAt: time1.Add(time.Hour * 2),
		},
	})
	assert.NoError(t, err)

	etcdhelper.AssertKVs(t, store.client.KV, `
<<<<<
stats/received/123/my-receiver/my-export/0001-01-01T00:01:00.000Z/0001-01-01T01:01:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T00:01:00Z",
  "sliceId": "0001-01-01T01:01:00Z",
  "nodeId": "my-node",
  "count": 111,
  "size": 1111,
  "lastReceivedAt": "0001-01-01T02:01:00Z"
}
>>>>>

<<<<<
stats/received/123/my-receiver/my-export/0001-01-01T12:01:00.000Z/0001-01-01T13:01:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T12:01:00Z",
  "sliceId": "0001-01-01T13:01:00Z",
  "nodeId": "my-node",
  "count": 222,
  "size": 2222,
  "lastReceivedAt": "0001-01-01T14:01:00Z"
}
>>>>>
`)
}
