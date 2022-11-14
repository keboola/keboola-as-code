package dependencies

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestConfigStore_CreateReceiver(t *testing.T) {
	t.Parallel()

	envs, _ := env.FromOs()

	if envs.Get("BUFFER_ETCD_ENABLED") == "false" {
		t.Skip()
	}

	// Setup
	ctx, d := newTestDeps(t)

	store := NewConfigStore(d.logger, d.etcdClient, d.validator)

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
		found = strings.HasPrefix(string(v.Key), fmt.Sprintf("config/%d/receiver/%s", config.ProjectID, config.ID))
		if found {
			assert.Equal(t, string(v.Value), encodedConfig)
		}
	}
	assert.True(t, found, "inserted config not found")
}

func newTestDeps(t *testing.T) (context.Context, *testDeps) {
	t.Helper()

	ctx := context.Background()
	d := &testDeps{
		logger:     log.NewDebugLogger(),
		etcdClient: newTestEtcdClient(t, ctx),
		validator:  validator.New(),
	}
	return ctx, d
}

type testDeps struct {
	logger     log.DebugLogger
	etcdClient *etcd.Client
	validator  validator.Validator
}

func newTestEtcdClient(t *testing.T, ctx context.Context) *etcd.Client {
	t.Helper()

	envs, err := env.FromOs()
	assert.NoError(t, err)

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
