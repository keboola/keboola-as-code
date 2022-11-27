package configstore

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create receiver
	config := model.Receiver{
		ID:        "github-pull-requests",
		ProjectID: 1000,
		Name:      "Github Pull Requests",
		Secret:    idgenerator.ReceiverSecret(),
	}
	err := store.CreateReceiver(ctx, config)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/1000/github-pull-requests
-----
{
  "receiverId": "github-pull-requests",
  "projectId": 1000,
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}

func TestStore_GetReceiver(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create receiver
	input := model.Receiver{
		ID:        "github-pull-requests",
		ProjectID: 1000,
		Name:      "Github Pull Requests",
		Secret:    idgenerator.ReceiverSecret(),
	}
	err := store.CreateReceiver(ctx, input)
	assert.NoError(t, err)

	// Get receiver
	receiver, err := store.GetReceiver(ctx, input.ProjectID, input.ID)
	assert.NoError(t, err)
	assert.Equal(t, input, receiver)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/1000/github-pull-requests
-----
{
  "receiverId": "github-pull-requests",
  "projectId": 1000,
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}

func TestStore_ListReceivers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	projectID := 1000

	// Create receivers
	input := []model.Receiver{
		{
			ID:        "github-pull-requests",
			ProjectID: projectID,
			Name:      "Github Pull Requests",
			Secret:    idgenerator.ReceiverSecret(),
		},
		{
			ID:        "github-issues",
			ProjectID: projectID,
			Name:      "Github Issues",
			Secret:    idgenerator.ReceiverSecret(),
		},
	}

	sort.SliceStable(input, func(i, j int) bool {
		return input[i].ID < input[j].ID
	})

	for _, r := range input {
		err := store.CreateReceiver(ctx, r)
		assert.NoError(t, err)
	}

	// List receivers
	receivers, err := store.ListReceivers(ctx, projectID)
	assert.NoError(t, err)

	sort.SliceStable(receivers, func(i, j int) bool {
		return receivers[i].ID < receivers[j].ID
	})
	assert.Equal(t, input, receivers)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/receiver/1000/github-issues
-----
{
  "receiverId": "github-issues",
  "projectId": 1000,
  "name": "Github Issues",
  "secret": "%s"
}
>>>>>

<<<<<
config/receiver/1000/github-pull-requests
-----
{
  "receiverId": "github-pull-requests",
  "projectId": 1000,
  "name": "Github Pull Requests",
  "secret": "%s"
}
>>>>>
`)
}
