package dependencies

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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

func TestConfigStore_CreateReceiver(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := NewConfigStore(d.logger, d.etcdClient, d.validator, d.tracer)

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

	found := false
	for _, v := range r.Kvs {
		found = strings.HasPrefix(string(v.Key), ReceiverKey(config.ProjectID, config.ID))
		if found {
			assert.Equal(t, string(v.Value), encodedConfig)
		}
	}
	assert.True(t, found, "inserted config not found")
}

func TestConfigStore_GetReceiver(t *testing.T) {
	t.Parallel()

	// Setup
	ctx, d := newTestDeps(t)
	store := NewConfigStore(d.logger, d.etcdClient, d.validator, d.tracer)

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
	store := NewConfigStore(d.logger, d.etcdClient, d.validator, d.tracer)

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
