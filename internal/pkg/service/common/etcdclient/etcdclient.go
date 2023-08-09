package etcdclient

import (
	"context"
	"strings"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	etcdNamespace "go.etcd.io/etcd/client/v3/namespace"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"         //nolint: depguard
	"go.uber.org/zap/zapcore" //nolint: depguard
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdlogger"
)

const (
	defaultConnectionTimeout = 10 * time.Second
	defaultKeepAliveTimeout  = 5 * time.Second
	defaultKeepAliveInterval = 10 * time.Second
)

type config struct {
	credentials       Credentials
	debugOpLogs       bool
	targetLeader      bool
	connectTimeout    time.Duration
	keepAliveTimeout  time.Duration
	keepAliveInterval time.Duration
	logger            log.Logger
}

type Option func(c *config)

func UseNamespace(c *etcd.Client, prefix string) {
	c.KV = etcdNamespace.NewKV(c.KV, prefix)
	c.Watcher = NewWatcher(c, prefix)
	c.Lease = etcdNamespace.NewLease(c.Lease, prefix)
}

// WithDebugOpLogs allows logging of each KV operation as a debug message.
func WithDebugOpLogs(v bool) Option {
	return func(c *config) {
		c.debugOpLogs = v
	}
}

// WithTargetLeader creates connections only to the leader node.
func WithTargetLeader(v bool) Option {
	return func(c *config) {
		c.targetLeader = v
	}
}

// WithConnectTimeout defines the maximum time for creating a connection in the New function.
func WithConnectTimeout(v time.Duration) Option {
	return func(c *config) {
		c.connectTimeout = v
	}
}

// WithDialTimeout defines the maximum time of one connection attempt.
// In case of failure, a retry follow.
func WithDialTimeout(v time.Duration) Option {
	return func(c *config) {
		c.connectTimeout = v
	}
}

func WithKeepAliveTimeout(v time.Duration) Option {
	return func(c *config) {
		c.keepAliveTimeout = v
	}
}

func WithKeepAliveInterval(v time.Duration) Option {
	return func(c *config) {
		c.keepAliveInterval = v
	}
}

// WithAutoSyncInterval defines how often the list of cluster nodes/endpoints will be synced.
// This is useful if the cluster will scale up or down.
func WithAutoSyncInterval(v time.Duration) Option {
	return func(c *config) {
		c.keepAliveTimeout = v
	}
}

func WithLogger(v log.Logger) Option {
	return func(c *config) {
		c.logger = v
	}
}

// New creates new etcd client.
// The client terminates the connection when the context is done.
func New(ctx context.Context, proc *servicectx.Process, tel telemetry.Telemetry, credentials Credentials, opts ...Option) (c *etcd.Client, err error) {
	ctx, span := tel.Tracer().Start(ctx, "keboola.go.common.dependencies.EtcdClient")
	defer span.End(&err)

	// Apply options
	cfg := config{
		credentials:       credentials,
		connectTimeout:    defaultConnectionTimeout,
		keepAliveTimeout:  defaultKeepAliveTimeout,
		keepAliveInterval: defaultKeepAliveInterval,
		logger:            log.NewNopLogger(),
	}
	for _, o := range opts {
		o(&cfg)
	}

	// Normalize and validate
	cfg.credentials.Normalize()
	if err := cfg.credentials.Validate(); err != nil {
		return nil, err
	}

	// Setup logger
	logger := cfg.logger.AddPrefix("[etcd-client]")
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// Create a zap logger for etcd client
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	etcdLogger := zap.New(log.NewCallbackCore(func(entry zapcore.Entry, fields []zapcore.Field) {
		// Skip debug messages
		if entry.Level == log.DebugLevel {
			return
		}

		// Add component=etcd-client field
		fields = append(fields, zapcore.Field{Key: "component", String: "etcd-client", Type: zapcore.StringType})

		// Encode and log message
		if bytes, err := encoder.EncodeEntry(entry, fields); err == nil {
			logger.Log(entry.Level.String(), strings.TrimRight(bytes.String(), "\n"))
		} else {
			logger.Warnf("cannot log msg from etcd client: %s", err)
		}
	}))

	// Create connect context
	connectCtx, connectCancel := context.WithTimeout(ctx, cfg.connectTimeout)
	defer connectCancel()

	// Create client
	startTime := time.Now()
	logger.Infof("connecting to etcd, connectTimeout=%s, keepAliveTimeout=%s, keepAliveInterval=%s", cfg.connectTimeout, cfg.keepAliveTimeout, cfg.keepAliveInterval)
	c, err = etcd.New(etcd.Config{
		Context:              context.Background(), // !!! a long-lived context must be used, client exists as long as the entire server
		Endpoints:            []string{cfg.credentials.Endpoint},
		DialTimeout:          cfg.connectTimeout,
		DialKeepAliveTimeout: cfg.keepAliveTimeout,
		DialKeepAliveTime:    cfg.keepAliveInterval,
		Username:             cfg.credentials.Username, // optional
		Password:             cfg.credentials.Password, // optional
		Logger:               etcdLogger,
		PermitWithoutStream:  true, // always send keep-alive pings
		DialOptions: []grpc.DialOption{
			grpc.WithChainUnaryInterceptor(otelgrpc.UnaryClientInterceptor(otelgrpc.WithTracerProvider(tel.TracerProvider()), otelgrpc.WithMeterProvider(tel.MeterProvider()))),
			grpc.WithChainStreamInterceptor(otelgrpc.StreamClientInterceptor(otelgrpc.WithTracerProvider(tel.TracerProvider()), otelgrpc.WithMeterProvider(tel.MeterProvider()))),
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
		return nil, errors.Errorf("cannot create etcd client: cannot connect: %w", err)
	}

	// Prefix client by namespace
	UseNamespace(c, cfg.credentials.Namespace)

	// Log each KV operation as a debug message, if enabled
	if cfg.debugOpLogs {
		c.KV = etcdlogger.KVLogWrapper(c.KV, logger.DebugWriter())
	}

	// Connect only to the leader node, if enabled
	if cfg.targetLeader {
		if eps, err := findLeaderEndpoints(ctx, c); err == nil {
			c.SetEndpoints(eps...)
		} else {
			return nil, err
		}
	}

	// Connection check: get cluster members
	if _, err := c.MemberList(connectCtx); err != nil {
		_ = c.Close()
		return nil, errors.Errorf("cannot create etcd client: cannot get cluster members: %w", err)
	}

	// Close client when shutting down the server
	proc.OnShutdown(func() {
		startTime := time.Now()
		logger.Info("closing etcd connection")
		if err := c.Close(); err != nil {
			logger.Warnf("cannot close etcd connection: %s", err)
		} else {
			logger.Infof("closed etcd connection | %s", time.Since(startTime))
		}
	})

	logger.Infof(`connected to etcd cluster "%s" | %s`, strings.Join(c.Endpoints(), ";"), time.Since(startTime))
	return c, nil
}

// findLeaderEndpoints inspired by https://github.com/etcd-io/etcd/blob/43f10cbd57b8b1c3f79f6efce99dd3b0b6a9e557/tools/benchmark/cmd/util.go#L44C6-L44C13
func findLeaderEndpoints(ctx context.Context, c *etcd.Client) (leaderEps []string, err error) {
	resp, lerr := c.MemberList(ctx)
	if lerr != nil {
		return nil, errors.New("failed to find a leader endpoint")
	}

	leaderID := uint64(0)
	for _, ep := range c.Endpoints() {
		if sresp, serr := c.Status(ctx, ep); serr == nil {
			leaderID = sresp.Leader
			break
		}
	}

	for _, m := range resp.Members {
		if m.ID == leaderID {
			leaderEps = m.ClientURLs
			return
		}
	}

	return nil, errors.New("failed to find a leader endpoint")
}
