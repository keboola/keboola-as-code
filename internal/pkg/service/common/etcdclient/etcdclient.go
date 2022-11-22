package etcdclient

import (
	"context"
	"strings"
	"sync"
	"time"

	etcd "go.etcd.io/etcd/client/v3"
	etcdNamespace "go.etcd.io/etcd/client/v3/namespace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"         //nolint: depguard
	"go.uber.org/zap/zapcore" //nolint: depguard
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	defaultConnectionTimeout = 2 * time.Second
	defaultKeepAliveTimeout  = 2 * time.Second
	defaultKeepAliveInterval = 10 * time.Second
	defaultAutoSyncInterval  = 1 * time.Minute
)

type config struct {
	endpoint          string
	username          string
	password          string
	namespace         string
	connectCtx        context.Context
	connectTimeout    time.Duration
	keepAliveTimeout  time.Duration
	keepAliveInterval time.Duration
	autoSyncInterval  time.Duration
	logger            log.Logger
	waitGroup         *sync.WaitGroup
}

type Option func(c *config)

func WithUsername(v string) Option {
	return func(c *config) {
		c.username = v
	}
}

func WithPassword(v string) Option {
	return func(c *config) {
		c.password = v
	}
}

// WithConnectContext allows you to use a different context within connection testing in the New function.
func WithConnectContext(v context.Context) Option {
	return func(c *config) {
		c.connectCtx = v
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

// WithWaitGroup attach the client to a waitGroup.
// When the connection is established, the waitGroup counter is increased,
// when the connection is closed, it is decreased.
// Connection is closed when the context is done, see New.
func WithWaitGroup(v *sync.WaitGroup) Option {
	return func(c *config) {
		c.waitGroup = v
	}
}

// New creates new etcd client.
// The client terminates the connection when the context is done.
func New(ctx context.Context, tracer trace.Tracer, endpoint, namespace string, opts ...Option) (c *etcd.Client, err error) {
	ctx, span := tracer.Start(ctx, "kac.api.server.templates.dependencies.EtcdClient")
	defer telemetry.EndSpan(span, &err)

	// Apply options
	conf := config{
		endpoint:          endpoint,
		namespace:         namespace,
		connectCtx:        ctx,
		connectTimeout:    defaultConnectionTimeout,
		keepAliveTimeout:  defaultKeepAliveTimeout,
		keepAliveInterval: defaultKeepAliveInterval,
		autoSyncInterval:  defaultAutoSyncInterval,
	}
	for _, o := range opts {
		o(&conf)
	}

	// Trim and validate
	endpoint = strings.Trim(endpoint, " /")
	if endpoint == "" {
		return nil, errors.New("etcd endpoint is not set")
	}
	namespace = strings.Trim(namespace, " /") + "/"
	if namespace == "/" {
		return nil, errors.New("etcd namespace is not set")
	}

	// Setup logger
	logger := conf.logger
	if logger == nil {
		logger = log.NewNopLogger()
	}

	// Create a zap logger for etcd client
	encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	etcdLogger := zap.New(log.NewCallbackCore(func(entry zapcore.Entry, fields []zapcore.Field) {
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
	connectCtx, connectCancel := context.WithTimeout(conf.connectCtx, conf.connectTimeout)
	defer connectCancel()

	// Create client
	startTime := time.Now()
	logger.Infof("connecting to etcd, connectTimeout=%s, keepAliveTimeout=%s, keepAliveInterval=%s", conf.connectTimeout, conf.keepAliveTimeout, conf.keepAliveInterval)
	c, err = etcd.New(etcd.Config{
		Context:              ctx, // !!! a long-lived context must be used, client exists as long as the entire server
		Endpoints:            []string{endpoint},
		AutoSyncInterval:     conf.autoSyncInterval,
		DialTimeout:          conf.connectTimeout,
		DialKeepAliveTimeout: conf.keepAliveTimeout,
		DialKeepAliveTime:    conf.keepAliveInterval,
		Username:             conf.username, // optional
		Password:             conf.password, // optional
		Logger:               etcdLogger,
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
		return nil, errors.Errorf("cannot create etcd client: cannot connect: %w", err)
	}

	// Prefix client by namespace
	c.KV = etcdNamespace.NewKV(c.KV, namespace)
	c.Watcher = etcdNamespace.NewWatcher(c.Watcher, namespace)
	c.Lease = etcdNamespace.NewLease(c.Lease, namespace)

	// Sync endpoints list from cluster, it is used also as a connection check.
	if err := c.Sync(connectCtx); err != nil {
		_ = c.Close()
		return nil, errors.Errorf("cannot create etcd client: cannot sync cluster members: %w", err)
	}

	// Close client when shutting down the server
	if conf.waitGroup != nil {
		conf.waitGroup.Add(1)
	}
	go func() {
		if conf.waitGroup != nil {
			defer conf.waitGroup.Done()
		}
		<-ctx.Done()
		if err := c.Close(); err != nil {
			logger.Warnf("cannot close connection etcd: %s", err)
		} else {
			logger.Info("closed connection to etcd")
		}
	}()

	logger.Infof(`connected to etcd cluster "%s" | %s`, strings.Join(c.Endpoints(), ";"), time.Since(startTime))
	return c, nil
}
