package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStore_CreateRecord(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := newStoreForTest(t)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"

	time1, err := time.Parse(time.RFC3339, `2006-01-01T15:04:05+07:00`)
	assert.NoError(t, err)
	time2, err := time.Parse(time.RFC3339, `2006-01-02T15:04:05+07:00`)
	assert.NoError(t, err)

	csv := []string{"one", "two", `th"ree`}
	record := key.NewRecordKey(projectID, receiverID, exportID, time1, time2)
	err = store.CreateRecord(ctx, record, csv)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, store.client, `
<<<<<
record/1000/receiver1/export1/2006-01-01T08:04:05.000Z/2006-01-02T08:04:05.000Z_%c%c%c%c%c
-----
one,two,"th""ree"
>>>>>
`)
}
