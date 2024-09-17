package etcdclient

import (
	"context"
	"io"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	etcdNamespace "go.etcd.io/etcd/client/v3/namespace"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.uber.org/zap"         //nolint: depguard
	"go.uber.org/zap/zapcore" //nolint: depguard
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

func UseNamespace(c *etcd.Client, prefix string) {
	c.KV = etcdNamespace.NewKV(c.KV, prefix)
	c.Watcher = NewWatcher(c, prefix)
	c.Lease = etcdNamespace.NewLease(c.Lease, prefix)
}

// New creates new etcd client.
// The client terminates the connection when the context is done.
func New(ctx context.Context, proc *servicectx.Process, tel telemetry.Telemetry, logger log.Logger, stderr io.Writer, cfg Config) (c *etcd.Client, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.common.dependencies.EtcdClient")
	defer span.End(&err)

	// Normalize and validate
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	// Setup logger
	if logger == nil {
		logger = log.NewNopLogger()
	}
	logger = logger.WithComponent("etcd.client")

	// Create a zap logger for etcd client
	etcdLogger := zap.New(
		logger.(log.LoggerWithZapCore).ZapCore(),
		// Add component=etcd.client field
		zap.Fields(zap.String("component", "etcd.client")),
		// Log stack trace for warnings/errors
		zap.AddStacktrace(zap.WarnLevel),
		// Skip debug messages
		zap.IncreaseLevel(zapcore.InfoLevel),
	)

	// Create connect context
	connectCtx, connectCancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer connectCancel()
	connectCtx = ctxattr.ContextWith(
		connectCtx,
		attribute.String("etcd.connect.timeout", cfg.ConnectTimeout.String()),
		attribute.String("etcd.keepAlive.timeout", cfg.KeepAliveTimeout.String()),
		attribute.String("etcd.keepAlive.interval", cfg.KeepAliveInterval.String()),
		attribute.StringSlice("etcd.endpoints", []string{cfg.Endpoint}),
	)

	// Create client
	startTime := time.Now()
	logger.Info(connectCtx, "connecting to etcd")
	c, err = etcd.New(etcd.Config{
		Context:              context.WithoutCancel(ctx), // !!! a long-lived context must be used, client exists as long as the entire server
		Endpoints:            []string{cfg.Endpoint},
		DialTimeout:          cfg.ConnectTimeout,
		DialKeepAliveTimeout: cfg.KeepAliveTimeout,
		DialKeepAliveTime:    cfg.KeepAliveInterval,
		Username:             cfg.Username, // optional
		Password:             cfg.Password, // optional
		Logger:               etcdLogger,
		PermitWithoutStream:  true, // always send keep-alive pings
		DialOptions: []grpc.DialOption{
			grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tel.TracerProvider()), otelgrpc.WithMeterProvider(tel.MeterProvider()))),
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
		return nil, errors.Errorf("cannot create etcd client: cannot connect: %w", err)
	}

	// Prefix client by namespace
	UseNamespace(c, cfg.Namespace)

	// Log each KV operation as a debug message, if enabled
	if cfg.DebugLog {
		c.KV = etcdlogger.KVLogWrapper(c.KV, stderr)
	}

	// Connection check: get cluster members
	if _, err := c.MemberList(connectCtx); err != nil {
		_ = c.Close()
		return nil, errors.Errorf("cannot create etcd client: cannot get cluster members: %w", err)
	}

	// Close client when shutting down the server
	proc.OnShutdown(func(ctx context.Context) {
		startTime := time.Now()
		logger.Info(ctx, "closing etcd connection")
		if err := c.Close(); err != nil {
			logger.Warnf(ctx, "cannot close etcd connection: %s", err)
		} else {
			logger.WithDuration(time.Since(startTime)).Infof(ctx, "closed etcd connection")
		}
	})

	logger.WithDuration(time.Since(startTime)).Infof(connectCtx, `connected to etcd cluster "%s"`, strings.Join(c.Endpoints(), ";"))
	return c, nil
}
