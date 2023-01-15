package store

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/filestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateReceiverOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	now, _ := time.Parse(time.RFC3339, "2006-01-01T08:04:05.000Z")
	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey1 := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export-1"}
	fileKey1 := key.FileKey{ExportKey: exportKey1, FileID: key.FileID(now)}
	sliceKey1 := key.SliceKey{FileKey: fileKey1, SliceID: key.SliceID(now)}
	mapping1 := model.Mapping{
		MappingKey: key.MappingKey{ExportKey: exportKey1, RevisionID: 1},
		TableID:    storageapi.MustParseTableID("in.c-bucket.table1"),
		Columns:    []column.Column{column.ID{Name: "id"}},
	}
	exportKey2 := key.ExportKey{ReceiverKey: receiverKey, ExportID: "my-export-2"}
	fileKey2 := key.FileKey{ExportKey: exportKey2, FileID: key.FileID(now)}
	sliceKey2 := key.SliceKey{FileKey: fileKey2, SliceID: key.SliceID(now)}
	mapping2 := model.Mapping{
		MappingKey:  key.MappingKey{ExportKey: exportKey2, RevisionID: 1},
		Incremental: true,
		TableID:     storageapi.MustParseTableID("in.c-bucket.table2"),
		Columns:     []column.Column{column.Body{Name: "body"}},
	}
	receiver := model.Receiver{
		ReceiverBase: model.ReceiverBase{
			ReceiverKey: receiverKey,
			Name:        "My Receiver",
			Secret:      "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
		},
		Exports: []model.Export{
			{
				ExportBase: model.ExportBase{
					ExportKey:        exportKey1,
					Name:             "My Export 1",
					ImportConditions: model.DefaultConditions(),
				},
				Mapping: mapping1,
				Token: model.Token{
					ExportKey:    exportKey1,
					StorageToken: storageapi.Token{Token: "my-token", ID: "1234"},
				},
				OpenedFile: model.File{
					FileKey:         fileKey1,
					State:           filestate.Opened,
					Mapping:         mapping1,
					StorageResource: &storageapi.File{},
				},
				OpenedSlice: model.Slice{
					SliceKey: sliceKey1,
					State:    slicestate.Opened,
					Mapping:  mapping1,
					Number:   1,
				},
			},
			{
				ExportBase: model.ExportBase{
					ExportKey:        exportKey2,
					Name:             "My Export 2",
					ImportConditions: model.DefaultConditions(),
				},
				Mapping: mapping2,
				Token: model.Token{
					ExportKey:    exportKey2,
					StorageToken: storageapi.Token{Token: "my-token", ID: "1234"},
				},
				OpenedFile: model.File{
					FileKey:         fileKey2,
					State:           filestate.Opened,
					Mapping:         mapping2,
					StorageResource: &storageapi.File{},
				},
				OpenedSlice: model.Slice{
					SliceKey: sliceKey2,
					State:    slicestate.Opened,
					Mapping:  mapping2,
					Number:   1,
				},
			},
		},
	}

	// Create and get
	assert.NoError(t, store.CreateReceiver(ctx, receiver))
	out, err := store.GetReceiver(ctx, receiverKey)
	assert.NoError(t, err)
	assert.Equal(t, receiver, out)
}

func TestStore_CreateReceiverBaseOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create receiver
	config := model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  1000,
			ReceiverID: "github-pull-requests",
		},
		Name:   "Github Pull Requests",
		Secret: idgenerator.ReceiverSecret(),
	}
	_, err := store.createReceiverBaseOp(ctx, config).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/00001000/github-pull-requests
-----
{
  "projectId": 1000,
  "receiverId": "github-pull-requests",
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}

func TestStore_GetReceiverBaseOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create receiver
	input := model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "github-pull-requests"},
		Name:        "Github Pull Requests",
		Secret:      idgenerator.ReceiverSecret(),
	}
	_, err := store.createReceiverBaseOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	// Get receiver
	receiver, err := store.getReceiverBaseOp(ctx, input.ReceiverKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, receiver.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/00001000/github-pull-requests
-----
{
  "projectId": 1000,
  "receiverId": "github-pull-requests",
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}

func TestStore_ListReceiversBaseOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	projectID := key.ProjectID(1000)

	// Create receivers
	input := []model.ReceiverBase{
		{
			ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "github-pull-requests"},
			Name:        "Github Pull Requests",
			Secret:      idgenerator.ReceiverSecret(),
		},
		{
			ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "github-issues"},
			Name:        "Github Issues",
			Secret:      idgenerator.ReceiverSecret(),
		},
	}

	sort.SliceStable(input, func(i, j int) bool {
		return input[i].ReceiverID < input[j].ReceiverID
	})

	for _, r := range input {
		_, err := store.createReceiverBaseOp(ctx, r).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	// List receivers
	receivers, err := store.receiversIterator(ctx, projectID).Do(ctx, store.client).All()
	assert.NoError(t, err)

	assert.Equal(t, input, receivers.Values())

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/00001000/github-issues
-----
{
  "projectId": 1000,
  "receiverId": "github-issues",
  "name": "Github Issues",
  "secret": "%s"
}
>>>>>

<<<<<
config/receiver/00001000/github-pull-requests
-----
{
  "projectId": 1000,
  "receiverId": "github-pull-requests",
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}

func TestStore_DeleteReceiverBaseOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create receiver
	input := model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{ProjectID: 1000, ReceiverID: "github-pull-requests"},
		Name:        "Github Pull Requests",
		Secret:      idgenerator.ReceiverSecret(),
	}
	_, err := store.createReceiverBaseOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	// Get receiver
	receiver, err := store.getReceiverBaseOp(ctx, input.ReceiverKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, receiver.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/00001000/github-pull-requests
-----
{
  "projectId": 1000,
  "receiverId": "github-pull-requests",
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}
