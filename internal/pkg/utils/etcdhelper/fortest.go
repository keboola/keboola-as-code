package etcdhelper

import (
	"context"
	"fmt"
	"os"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"         //nolint: depguard
	"go.uber.org/zap/zapcore" //nolint: depguard
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testOrBenchmark interface {
	Cleanup(f func())
	Skipf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func ClientForTest(t testOrBenchmark) *etcd.Client {
	envs, err := env.FromOs()
	if err != nil {
		t.Fatalf("cannot get envs: %s", err)
	}

	if envs.Get("UNIT_ETCD_ENABLED") == "false" {
		t.Skipf("etcd test is disabled by UNIT_ETCD_ENABLED=false")
	}

	endpoint := envs.Get("UNIT_ETCD_ENDPOINT")
	username := envs.Get("UNIT_ETCD_USERNAME")
	password := envs.Get("UNIT_ETCD_PASSWORD")
	prefix := fmt.Sprintf("unit-%s/", idgenerator.EtcdNamespaceForTest())
	return ClientForTestFrom(t, endpoint, username, password, prefix)
}

func ClientForTestFrom(t testOrBenchmark, endpoint, username, password, prefix string) *etcd.Client {
	ctx := context.Background()
	if endpoint == "" {
		t.Fatalf(`etcd endpoint is not set`)
	}

	// Setup logger
	var logger *zap.Logger
	if testhelper.TestIsVerbose() {
		encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		logger = zap.New(log.NewCallbackCore(func(entry zapcore.Entry, fields []zapcore.Field) {
			if entry.Level > log.DebugLevel {
				bytes, _ := encoder.EncodeEntry(entry, fields)
				_, _ = os.Stdout.Write(bytes.Bytes())
			}
		}))
	}

	// Create etcd client
	etcdClient, err := etcd.New(etcd.Config{
		Context:              ctx,
		Endpoints:            []string{endpoint},
		DialTimeout:          2 * time.Second,
		DialKeepAliveTimeout: 2 * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		Username:             username, // optional
		Password:             password, // optional
		Logger:               logger,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(), // wait for the connection
			grpc.WithReturnConnectionError(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: backoff.Config{
					BaseDelay:  100 * time.Millisecond,
					Multiplier: 1.5,
					Jitter:     0.2,
					MaxDelay:   15 * time.Second,
				},
			}),
		},
	})
	if err != nil {
		t.Fatalf("cannot create etcd client: %s", err)
	}

	// Create namespace
	originalKV := etcdClient.KV // not namespaced client for the cleanup
	etcdClient.KV = namespace.NewKV(etcdClient.KV, prefix)
	etcdClient.Lease = namespace.NewLease(etcdClient.Lease, prefix)
	etcdClient.Watcher = namespace.NewWatcher(etcdClient.Watcher, prefix)

	// Add operations logger
	etcdClient.KV = KVLogWrapper(etcdClient.KV, testhelper.VerboseStdout())

	// Cleanup namespace after the test
	t.Cleanup(func() {
		_, err := originalKV.Delete(ctx, prefix, etcd.WithPrefix())
		if err != nil {
			t.Fatalf(`cannot clear etcd namespace "%s" after test: %s`, prefix, err)
		}
	})

	return etcdClient
}
