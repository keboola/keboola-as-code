package configstore

import (
	"context"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateExport(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)
	projectID := 1000
	receiverID := "github"

	config := model.Export{
		ID:   "github-issues",
		Name: "Github Issues",
		ImportConditions: model.ImportConditions{
			Count: 5,
			Size:  datasize.MustParseString("50kB"),
			Time:  30 * time.Minute,
		},
	}
	err := store.CreateExport(ctx, projectID, receiverID, config)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/github/github-issues
-----
{
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
	projectID := 1000
	receiverID := "receiver1"

	// Create exports
	input := []model.Export{
		{
			ID:   "export-1",
			Name: "Export 1",
			ImportConditions: model.ImportConditions{
				Count: 5,
				Size:  datasize.MustParseString("50kB"),
				Time:  30 * time.Minute,
			},
		},
		{
			ID:   "export-2",
			Name: "Export 2",
			ImportConditions: model.ImportConditions{
				Count: 5,
				Size:  datasize.MustParseString("50kB"),
				Time:  5 * time.Minute,
			},
		},
	}

	for _, e := range input {
		err := store.CreateExport(ctx, projectID, receiverID, e)
		assert.NoError(t, err)
	}

	// List
	output, err := store.ListExports(ctx, projectID, receiverID)
	assert.NoError(t, err)
	assert.Equal(t, input, output)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/1000/receiver1/export-1
-----
{
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
