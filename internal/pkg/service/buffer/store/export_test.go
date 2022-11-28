package store

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateExport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	export := model.ExportBase{
		ExportKey: exportKey,
		Name:      "Github Issues",
		ImportConditions: model.ImportConditions{
			Count: 5,
			Size:  datasize.MustParseString("50kB"),
			Time:  30 * time.Minute,
		},
	}
	err := store.CreateExport(ctx, export)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/github/github-issues
-----
{
  "projectId": 1000,
  "receiverId": "github",
  "exportId": "github-issues",
  "name": "Github Issues",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 1800000000000
  }
}
>>>>>
`)
}

func TestStore_ListExports(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "receiver1"}

	// Create exports
	input := []model.ExportBase{
		{
			ExportKey: key.ExportKey{ExportID: "export-1", ReceiverKey: receiverKey},
			Name:      "Export 1",
			ImportConditions: model.ImportConditions{
				Count: 5,
				Size:  datasize.MustParseString("50kB"),
				Time:  30 * time.Minute,
			},
		},
		{
			ExportKey: key.ExportKey{ExportID: "export-2", ReceiverKey: receiverKey},
			Name:      "Export 2",
			ImportConditions: model.ImportConditions{
				Count: 5,
				Size:  datasize.MustParseString("50kB"),
				Time:  5 * time.Minute,
			},
		},
	}

	for _, e := range input {
		err := store.CreateExport(ctx, e)
		assert.NoError(t, err)
	}

	// List
	output, err := store.ListExports(ctx, receiverKey)
	assert.NoError(t, err)
	assert.Equal(t, input, output)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/receiver1/export-1
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-1",
  "name": "Export 1",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 1800000000000
  }
}
>>>>>

<<<<<
config/export/1000/receiver1/export-2
-----
{
  "projectId": 1000,
  "receiverId": "receiver1",
  "exportId": "export-2",
  "name": "Export 2",
  "importConditions": {
    "count": 5,
    "size": "50KB",
    "time": 300000000000
  }
}
>>>>>
`)
}
