package configstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model"
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

	// Assert that it exists in the DB
	encodedConfig, err := json.EncodeString(config, false)
	assert.NoError(t, err)

	r, err := d.etcdClient.KV.Get(ctx, "config", etcd.WithPrefix())
	assert.NoError(t, err)

	var found *string
	for _, v := range r.Kvs {
		if strings.HasPrefix(string(v.Key), ReceiverKey(config.ProjectID, config.ID)) {
			temp := string(v.Value)
			found = &temp
			break
		}
	}
	assert.Equal(t, *found, encodedConfig, "inserted config not found")
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
			Count: 1,
			Size:  100,
		},
	}
	err := store.CreateExport(ctx, projectID, receiverID, config)
	assert.NoError(t, err)

	encodedConfig, err := json.EncodeString(config, false)
	assert.NoError(t, err)

	r, err := d.etcdClient.KV.Get(ctx, "config", etcd.WithPrefix())
	assert.NoError(t, err)

	var found *string
	for _, v := range r.Kvs {
		if strings.HasPrefix(string(v.Key), ExportKey(projectID, receiverID, config.ID)) {
			temp := string(v.Value)
			found = &temp
			break
		}
	}
	assert.Equal(t, *found, encodedConfig, "inserted config not found")
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
			},
		},
		{
			ID:   "export-2",
			Name: "Export 2",
			ImportConditions: model.ImportConditions{
				Time: 5 * time.Minute,
			},
		},
	}

	for _, i := range input {
		key := fmt.Sprintf("config/export/%d/%s/%s", projectID, receiverID, i.ID)
		value, err := json.EncodeString(i, false)
		assert.NoError(t, err)
		_, err = d.etcdClient.KV.Put(ctx, key, value)
		assert.NoError(t, err)
	}

	// List
	exports, err := store.ListExports(ctx, projectID, receiverID)
	assert.NoError(t, err)

	assert.Equal(t, input, exports)
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
	input := []*model.Mapping{
		{
			RevisionID:  1,
			TableID:     tableID,
			Incremental: false,
			Columns:     nil,
		},
		{
			RevisionID:  2,
			TableID:     tableID,
			Incremental: false,
			Columns:     nil,
		},
		{
			RevisionID:  10,
			TableID:     tableID,
			Incremental: false,
			Columns:     nil,
		},
	}

	for _, i := range input {
		value, err := json.EncodeString(i, false)
		assert.NoError(t, err)
		_, err = d.etcdClient.KV.Put(ctx, MappingKey(projectID, receiverID, exportID, i.RevisionID), value)
		assert.NoError(t, err)
	}

	// Get current mapping
	mapping, err := store.GetCurrentMapping(ctx, projectID, receiverID, exportID)
	assert.NoError(t, err)

	assert.Equal(t, input[2], mapping)
}

func TestConfigStore_CreateRecord(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := New(d.logger, d.etcdClient, d.validator, d.tracer)

	projectID := 1000
	receiverID := "receiver1"
	exportID := "export1"

	csv := []string{"one", "two", `th"ree`}
	record := RecordKey{
		projectID:  projectID,
		receiverID: receiverID,
		exportID:   exportID,
		fileID:     "file1",
		sliceID:    "slice1",
		receivedAt: time.Now(),
	}

	err := store.CreateRecord(ctx, record, csv)
	assert.NoError(t, err)

	r, err := d.etcdClient.KV.Get(ctx, "record/1000/receiver1/export1/file1/slice1/"+FormatTimeForKey(record.receivedAt), etcd.WithPrefix())
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r.Kvs))
	assert.Equal(t, "one,two,\"th\"\"ree\"\n", string(r.Kvs[0].Value))
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
		etcdClient: newTestEtcdClient(t, ctx),
		validator:  validator.New(),
		tracer:     trace.NewNoopTracerProvider().Tracer(""),
	}
	return ctx, d
}

func newTestEtcdClient(t *testing.T, ctx context.Context) *etcd.Client {
	t.Helper()

	envs, err := env.FromOs()
	assert.NoError(t, err)

	if envs.Get("BUFFER_ETCD_ENABLED") == "false" {
		t.Skip()
	}

	// Create etcd client
	etcdClient, err := etcd.New(etcd.Config{
		Context:              context.Background(),
		Endpoints:            []string{envs.Get("BUFFER_ETCD_ENDPOINT")},
		DialTimeout:          2 * time.Second,
		DialKeepAliveTimeout: 2 * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		Username:             envs.Get("BUFFER_ETCD_USERNAME"), // optional
		Password:             envs.Get("BUFFER_ETCD_PASSWORD"), // optional
	})
	assert.NoError(t, err)

	prefix := fmt.Sprintf("buffer-%s/", idgenerator.EtcdNamespaceForTest())
	etcdClient.KV = namespace.NewKV(etcdClient.KV, prefix)

	t.Cleanup(func() {
		etcdClient.KV.Delete(ctx, prefix, etcd.WithPrefix())
	})

	return etcdClient
}
