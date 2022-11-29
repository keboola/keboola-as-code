package store

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

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
config/receiver/1000/github-pull-requests
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
config/receiver/1000/github-pull-requests
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

	projectID := 1000

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
	receivers, err := store.listReceiversBaseOp(ctx, projectID).Do(ctx, store.client)
	assert.NoError(t, err)

	assert.Equal(t, input, receivers.Values())

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/1000/github-issues
-----
{
  "projectId": 1000,
  "receiverId": "github-issues",
  "name": "Github Issues",
  "secret": "%s"
}
>>>>>

<<<<<
config/receiver/1000/github-pull-requests
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
config/receiver/1000/github-pull-requests
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
