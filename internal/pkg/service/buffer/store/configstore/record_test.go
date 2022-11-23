package configstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

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
	record := model.RecordKey{
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
