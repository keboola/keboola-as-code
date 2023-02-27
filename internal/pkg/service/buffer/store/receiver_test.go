package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateReceiverOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	// Create and get
	receiver := model.ReceiverForTest("my-receiver", 2, time.Time{})
	assert.NoError(t, store.CreateReceiver(ctx, receiver))
	out, err := store.GetReceiver(ctx, receiver.ReceiverKey)
	assert.NoError(t, err)
	assert.Equal(t, receiver, out)
}

func TestStore_CreateReceiverOp_MaxReceiversCount(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	start := make(chan struct{})
	store := newStoreForTest(t)

	overflow := 10
	for i := 0; i < MaxReceiversPerProject+overflow; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			receiver := model.ReceiverForTest(fmt.Sprintf("my-receiver-%03d", i), 0, time.Time{})
			if err := store.CreateReceiver(ctx, receiver); err != nil {
				errs.Append(err)
			}
		}()
	}

	close(start)
	wg.Wait()

	// Number of the errors must match
	assert.Equal(t, overflow, errs.Len())
	for _, err := range errs.WrappedErrors() {
		assert.Equal(t, "receiver count limit reached in the project, the maximum is 100", err.Error())
	}
}

func TestStore_CreateReceiverOp_MaxExportsCount(t *testing.T) {
	t.Parallel()

	store := newStoreForTest(t)

	exportsCount := MaxExportsPerReceiver + 1
	receiver := model.ReceiverForTest("my-receiver", exportsCount, time.Time{})
	err := store.CreateReceiver(context.Background(), receiver)
	assert.Error(t, err)
	assert.Equal(t, "export count limit reached in the receiver, the maximum is 20", err.Error())
}

func TestStore_CheckCreateReceiver_Exists(t *testing.T) {
	t.Parallel()

	store := newStoreForTest(t)
	receiver := model.ReceiverForTest("my-receiver", 1, time.Time{})

	// Check passes because there is no such receiver in the store.
	err := store.CheckCreateReceiver(context.Background(), receiver.ReceiverKey)
	assert.NoError(t, err)

	err = store.CreateReceiver(context.Background(), receiver)
	assert.NoError(t, err)

	// Check fails because there already is the same receiver in the store.
	err = store.CheckCreateReceiver(context.Background(), receiver.ReceiverKey)
	assert.Equal(t, `receiver "my-receiver" already exists in the project`, err.Error())
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
	etcdhelper.AssertKVsString(t, store.client, `
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
	etcdhelper.AssertKVsString(t, store.client, `
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
	etcdhelper.AssertKVsString(t, store.client, `
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
	etcdhelper.AssertKVsString(t, store.client, `
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
