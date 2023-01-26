package etcdhelper

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
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
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testOrBenchmark interface {
	Cleanup(f func())
	Skipf(format string, args ...any)
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func NamespaceForTest() string {
	return idgenerator.EtcdNamespaceForTest()
}

func ClientForTest(t testOrBenchmark, dialOpts ...grpc.DialOption) *etcd.Client {
	namespaceStr := fmt.Sprintf("unit-%s/", NamespaceForTest())
	return ClientForTestWithNamespace(t, namespaceStr, dialOpts...)
}

func ClientForTestWithNamespace(t testOrBenchmark, namespaceStr string, dialOpts ...grpc.DialOption) *etcd.Client {
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

	return ClientForTestFrom(t, endpoint, username, password, namespaceStr, dialOpts...)
}

func ClientForTestFrom(t testOrBenchmark, endpoint, username, password, namespaceStr string, dialOpts ...grpc.DialOption) *etcd.Client {
	ctx, cancel := context.WithCancel(context.Background())
	if endpoint == "" {
		t.Fatalf(`etcd endpoint is not set`)
	}

	// Setup logger
	var logger *zap.Logger

	// Should be logger enabled?
	verboseStr, found := os.LookupEnv("ETCD_VERBOSE")
	verbose := found && strings.ToLower(verboseStr) == "true"
	if !found {
		verbose = testhelper.TestIsVerbose()
	}

	// Enable logger
	if verbose {
		encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
		logger = zap.New(log.NewCallbackCore(func(entry zapcore.Entry, fields []zapcore.Field) {
			if entry.Level > log.DebugLevel {
				bytes, _ := encoder.EncodeEntry(entry, fields)
				_, _ = os.Stdout.Write(bytes.Bytes())
			}
		}))
	}

	// Dial options
	dialOpts = append(
		dialOpts,
		grpc.WithBlock(),                 // wait for the connection
		grpc.WithReturnConnectionError(), // wait for the connection error
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.5,
				Jitter:     0.2,
				MaxDelay:   15 * time.Second,
			},
		}),
	)

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
		DialOptions:          dialOpts,
	})
	if err != nil {
		t.Fatalf("cannot create etcd client: %s, %s", err, debug.Stack())
	}

	// Create namespace
	originalClient := etcdClient // not namespaced client for the cleanup
	etcdClient.KV = namespace.NewKV(etcdClient.KV, namespaceStr)
	etcdClient.Lease = namespace.NewLease(etcdClient.Lease, namespaceStr)
	etcdClient.Watcher = namespace.NewWatcher(etcdClient.Watcher, namespaceStr)

	// Add operations logger
	if verbose {
		etcdClient.KV = etcdlogger.KVLogWrapper(etcdClient.KV, os.Stdout)
	}

	// Cleanup namespace after the test
	t.Cleanup(func() {
		_, err := originalClient.Delete(ctx, namespaceStr, etcd.WithPrefix())
		if err != nil {
			t.Fatalf(`cannot clear etcd namespace "%s" after test: %s`, namespaceStr, err)
		}

		// Close context after second, so running request can finish.
		// It prevents warnings in the test console.
		go func() {
			<-time.After(time.Second)
			cancel()
		}()
	})

	return etcdClient
}
