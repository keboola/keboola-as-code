package recordstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"

	now, err := time.Parse(time.RFC3339, `2006-01-02T15:04:05+07:00`)
	assert.NoError(t, err)

	csv := []string{"one", "two", `th"ree`}
	record := schema.RecordKey{
		ProjectID:  projectID,
		ReceiverID: receiverID,
		ExportID:   exportID,
		FileID:     "file1",
		SliceID:    "slice1",
		ReceivedAt: now,
	}

	err = store.CreateRecord(ctx, record, csv)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.etcdClient, `
<<<<<
record/1000/receiver1/export1/file1/slice1/2006-01-02T08:04:05.000Z_%c%c%c%c%c
-----
one,two,"th""ree"
>>>>>
`)
}
