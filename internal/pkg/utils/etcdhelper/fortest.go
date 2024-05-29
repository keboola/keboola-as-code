package etcdhelper

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"         //nolint: depguard
	"go.uber.org/zap/zapcore" //nolint: depguard
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

type testOrBenchmark interface {
	Cleanup(f func())
	Skipf(format string, args ...any)
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// TmpNamespace creates a temporary etcd namespace and registers cleanup after the test.
func TmpNamespace(t testOrBenchmark) etcdclient.Config {
	return TmpNamespaceFromEnv(t, "UNIT_ETCD_")
}

// TmpNamespaceFromEnv creates a temporary etcd namespace and registers cleanup after the test.
// Config are read from the provided ENV prefix.
func TmpNamespaceFromEnv(t testOrBenchmark, envPrefix string) etcdclient.Config {
	envs, err := env.FromOs()
	if err != nil {
		t.Fatalf("cannot get envs: %s", err)
	}

	if envs.Get(envPrefix+"ENABLED") == "false" {
		t.Skipf(fmt.Sprintf("etcd test is disabled by %s_ENABLED=false", envPrefix))
	}

	cfg := etcdclient.NewConfig()
	cfg.Endpoint = envs.Get(envPrefix + "ENDPOINT")
	cfg.Namespace = idgenerator.EtcdNamespaceForTest()
	cfg.Username = envs.Get(envPrefix + "USERNAME")
	cfg.Password = envs.Get(envPrefix + "PASSWORD")

	if cfg.Endpoint == "" {
		t.Fatalf(`etcd endpoint env "%s" is not set`, envPrefix+"ENDPOINT")
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		client := clientForTest(t, ctx, cfg)
		_, err := client.Delete(ctx, "", etcd.WithFromKey())
		cancel()
		if err != nil {
			t.Fatalf(`cannot clear etcd after test: %s`, err)
		}
	})

	return cfg
}

func ClientForTest(t testOrBenchmark, cfg etcdclient.Config, dialOpts ...grpc.DialOption) *etcd.Client {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
	})
	return clientForTest(t, ctx, cfg, dialOpts...)
}

func clientForTest(t testOrBenchmark, ctx context.Context, cfg etcdclient.Config, dialOpts ...grpc.DialOption) *etcd.Client {
	// Normalize namespace
	cfg.Namespace = strings.Trim(cfg.Namespace, " /") + "/"

	// Setup logger
	var logger *zap.Logger

	// Should be logger enabled?
	verbose := VerboseTestLogs()

	// Replace default logger
	// By default only client errors are printed to the test stdout.
	// Each server error, for example "etcdserver: duplicate key given in txn request",
	// is also client warning, but these errors are checked in the tests,
	// so we usually do not need to log them in duplicate from the client.
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	logger = zap.New(log.NewCallbackCore(func(entry zapcore.Entry, fields []zapcore.Field) {
		minLevel := log.ErrorLevel
		if verbose {
			minLevel = log.InfoLevel
		}
		if entry.Level >= minLevel {
			bytes, _ := encoder.EncodeEntry(entry, fields)
			_, _ = os.Stdout.WriteString("ETCD_TEST_CLIENT " + bytes.String() + "\n") // nolint:forbidigo
		}
	}))

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
				MaxDelay:   5 * time.Second,
			},
		}),
	)

	// Create etcd client
	etcdClient, err := etcd.New(etcd.Config{
		Context:              ctx,
		Endpoints:            []string{cfg.Endpoint},
		DialTimeout:          15 * time.Second,
		DialKeepAliveTimeout: 5 * time.Second,
		DialKeepAliveTime:    10 * time.Second,
		Username:             cfg.Username, // optional
		Password:             cfg.Password, // optional
		Logger:               logger,
		DialOptions:          dialOpts,
	})
	if err != nil {
		t.Fatalf("cannot create etcd client: %s, %s", err, debug.Stack())
	}

	// Use namespace
	etcdclient.UseNamespace(etcdClient, cfg.Namespace)

	// Add operations logger
	if verbose {
		etcdClient.KV = etcdlogger.KVLogWrapper(etcdClient.KV, os.Stdout) // nolint:forbidigo
	}

	return etcdClient
}

func VerboseTestLogs() bool {
	str, found := os.LookupEnv("ETCD_VERBOSE")
	return found && strings.ToLower(str) == "true"
}
