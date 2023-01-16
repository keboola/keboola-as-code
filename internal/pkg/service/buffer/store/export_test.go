package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateExportBaseOp(t *testing.T) {
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
	_, err := store.createExportBaseOp(ctx, export).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/00001000/github/github-issues
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

func TestStore_CreateExport_MaxCount(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()
	start := make(chan struct{})
	store := newStoreForTest(t)

	receiver := model.ReceiverForTest("my-receiver", 0, time.Time{})
	assert.NoError(t, store.CreateReceiver(ctx, receiver))
	assert.Len(t, receiver.Exports, 0)

	overflow := 10
	for i := 0; i < MaxExportsPerReceiver+overflow; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			exportID := fmt.Sprintf("my-export-%03d", i)
			tableID := fmt.Sprintf("in.c-bucket.table%03d", i)
			columns := []column.Column{column.ID{Name: "col1"}, column.Body{Name: "col2"}}
			export := model.ExportForTest(receiver.ReceiverKey, exportID, tableID, columns, time.Time{})
			if err := store.CreateExport(ctx, export); err != nil {
				errs.Append(err)
			}
		}()
	}

	close(start)
	wg.Wait()

	// Number of the errors must match
	assert.Equal(t, overflow, errs.Len())
	for _, err := range errs.WrappedErrors() {
		assert.Equal(t, "export count limit reached in the receiver, the maximum is 20", err.Error())
	}
}

func TestStore_GetExportBaseOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	receiverKey := key.ReceiverKey{ProjectID: 1000, ReceiverID: "github"}
	exportKey := key.ExportKey{ExportID: "github-issues", ReceiverKey: receiverKey}
	input := model.ExportBase{
		ExportKey: exportKey,
		Name:      "Github Issues",
		ImportConditions: model.ImportConditions{
			Count: 5,
			Size:  datasize.MustParseString("50kB"),
			Time:  30 * time.Minute,
		},
	}
	_, err := store.createExportBaseOp(ctx, input).Do(ctx, store.client)
	assert.NoError(t, err)

	kv, err := store.getExportBaseOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)
	assert.Equal(t, input, kv.Value)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/00001000/github/github-issues
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

func TestStore_ListExportsBaseOp(t *testing.T) {
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
		_, err := store.createExportBaseOp(ctx, e).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	// List
	kvs, err := store.exportBaseIterator(ctx, receiverKey).Do(ctx, store.client).All()
	assert.NoError(t, err)
	assert.Equal(t, input, kvs.Values())

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/00001000/receiver1/export-1
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
config/export/00001000/receiver1/export-2
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

func TestStore_DeleteExportBaseOp(t *testing.T) {
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
	_, err := store.createExportBaseOp(ctx, export).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/00001000/github/github-issues
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

	_, err = store.deleteExportBaseOp(ctx, exportKey).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, ``)
}

func TestStore_DeleteExportBaseListOp(t *testing.T) {
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
		_, err := store.createExportBaseOp(ctx, e).Do(ctx, store.client)
		assert.NoError(t, err)
	}

	// List
	kvs, err := store.exportBaseIterator(ctx, receiverKey).Do(ctx, store.client).All()
	assert.NoError(t, err)
	assert.Equal(t, input, kvs.Values())

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
config/export/00001000/receiver1/export-1
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
config/export/00001000/receiver1/export-2
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

	_, err = store.deleteReceiverExportsOp(ctx, receiverKey).Do(ctx, store.client)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, ``)
}
