package configstore

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestReceiverPrefix(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "config/receiver/1000", ReceiverPrefix(1000))
}

func TestReceiverKey(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "config/receiver/1000/asdf", ReceiverKey(1000, "asdf"))
}

func TestRecordKey(t *testing.T) {
	t.Parallel()

	key := RecordKey{
		projectID:  1000,
		receiverID: "asdf",
		exportID:   "exp123",
		fileID:     "file456",
		sliceID:    "slice789",
		receivedAt: time.Now(),
	}

	assert.True(t, strings.HasPrefix(key.String(), "record/1000/asdf/exp123/file456/slice789/"+FormatTimeForKey(key.receivedAt)))
	assert.NotEqual(t, key.String(), key.String())
}

func TestConfigStore_CreateReceiver(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

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
	etcdhelper.AssertKVs(t, d.etcdClient, `
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

func TestConfigStore_GetReceiver(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	// Create receiver
	input := &model.Receiver{
		ID:        "github-pull-requests",
		ProjectID: 1000,
		Name:      "Github Pull Requests",
		Secret:    idgenerator.ReceiverSecret(),
	}
	err := store.CreateReceiver(ctx, *input)
	assert.NoError(t, err)

	// Get receiver
	receiver, err := store.GetReceiver(ctx, input.ProjectID, input.ID)
	assert.NoError(t, err)
	assert.Equal(t, input, receiver)

	// Check keys
	etcdhelper.AssertKVs(t, d.etcdClient, `
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

func TestConfigStore_ListReceivers(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000

	// Create receivers
	input := []*model.Receiver{
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
		err := store.CreateReceiver(ctx, *r)
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
	etcdhelper.AssertKVs(t, d.etcdClient, `
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

func TestConfigStore_CreateExport(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

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
	etcdhelper.AssertKVs(t, d.etcdClient, `
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

func TestConfigStore_ListExports(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000
	receiverID := "receiver1"

	// Create exports
	input := []*model.Export{
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
		err := store.CreateExport(ctx, projectID, receiverID, *e)
		assert.NoError(t, err)
	}

	// List
	output, err := store.ListExports(ctx, projectID, receiverID)
	assert.NoError(t, err)
	assert.Equal(t, input, output)

	// Check keys
	etcdhelper.AssertKVs(t, d.etcdClient, `
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

func TestConfigStore_GetCurrentMapping(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"
	tableID := model.TableID{
		Stage:  model.TableStageIn,
		Bucket: "main",
		Table:  "table1",
	}

	// Create mapppings
	input := []model.Mapping{
		{
			RevisionID:  1,
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{}},
		},
		{
			RevisionID:  2,
			TableID:     tableID,
			Incremental: false,
			Columns:     column.Columns{column.ID{}},
		},
		{
			RevisionID:  10,
			TableID:     tableID,
			Incremental: true,
			Columns:     column.Columns{column.ID{}},
		},
	}

	for _, m := range input {
		err := store.CreateMapping(ctx, projectID, receiverID, exportID, m)
		assert.NoError(t, err)
	}

	// Get current mapping
	mapping, err := store.GetCurrentMapping(ctx, projectID, receiverID, exportID)
	assert.NoError(t, err)
	assert.Equal(t, &input[2], mapping)

	// Check keys
	etcdhelper.AssertKVs(t, d.etcdClient, `
<<<<<
config/mapping/revision/1000/receiver1/export1/00000001
-----
{
  "revisionId": 1,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": false,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000002
-----
{
  "revisionId": 2,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": false,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>

<<<<<
config/mapping/revision/1000/receiver1/export1/00000010
-----
{
  "revisionId": 10,
  "tableId": {
    "stage": "in",
    "bucketName": "main",
    "tableName": "table1"
  },
  "incremental": true,
  "columns": [
    {
      "type": "id"
    }
  ]
}
>>>>>
`)
}

func TestConfigStore_CreateRecord(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"

	now, err := time.Parse(time.RFC3339, `2006-01-02T15:04:05+07:00`)
	assert.NoError(t, err)

	csv := []string{"one", "two", `th"ree`}
	record := RecordKey{
		projectID:  projectID,
		receiverID: receiverID,
		exportID:   exportID,
		fileID:     "file1",
		sliceID:    "slice1",
		receivedAt: now,
	}

	err = store.CreateRecord(ctx, record, csv)
	assert.NoError(t, err)

	// Check keys
	etcdhelper.AssertKVs(t, d.etcdClient, `
<<<<<
record/1000/receiver1/export1/file1/slice1/2006-01-02T08:04:05.000Z_%c%c%c%c%c
-----
one,two,"th""ree"
>>>>>
`)
}

type testDeps struct {
	logger     log.DebugLogger
	etcdClient *etcd.Client
	validator  validator.Validator
	tracer     trace.Tracer
}

func newTestDeps(t *testing.T) (context.Context, *testDeps) {
	t.Helper()

	ctx := context.Background()
	d := &testDeps{
		logger:     log.NewDebugLogger(),
		etcdClient: etcdhelper.ClientForTest(t),
		validator:  validator.New(),
		tracer:     trace.NewNoopTracerProvider().Tracer(""),
	}
	return ctx, d
}
