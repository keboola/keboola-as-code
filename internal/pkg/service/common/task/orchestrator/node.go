package orchestrator

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Node struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	clock  clock.Clock
	logger log.Logger
	tracer telemetry.Tracer
	client *etcd.Client
	tasks  *task.Node
	dist   *distribution.Node
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
	EtcdClient() *etcd.Client
	TaskNode() *task.Node
	DistributionNode() *distribution.Node
}

// config is interface for generic type Config[T].
type configInterface interface {
	newOrchestrator(node *Node) orchestratorInterface
}

// orchestrator is interface for generic type orchestrator[T].
type orchestratorInterface interface {
	start() <-chan error
}

func NewNode(d dependencies) *Node {
	n := &Node{
		clock:  d.Clock(),
		logger: d.Logger().WithComponent("orchestrator"),
		tracer: d.Telemetry().Tracer(),
		client: d.EtcdClient(),
		tasks:  d.TaskNode(),
		dist:   d.DistributionNode(),
	}

	// Graceful shutdown
	var cancel context.CancelFunc
	n.ctx, cancel = context.WithCancel(context.Background()) // nolint: contextcheck
	n.wg = &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		n.logger.InfoCtx(ctx, "received shutdown request")
		cancel()
		n.logger.InfoCtx(ctx, `waiting for orchestrators to finish`)
		n.wg.Wait()
		n.logger.InfoCtx(ctx, "shutdown done")
	})

	return n
}

// Start a new orchestrator.
// The returned channel signals completion of initialization and return an error if one occurred.
// If an error occurs during execution, after successful initialization, it retries until the error is resolved.
func (n *Node) Start(config configInterface) <-chan error {
	return config.newOrchestrator(n).start()
}

func (c Config[T]) newOrchestrator(node *Node) orchestratorInterface {
	// Validate config
	if err := c.Validate(); err != nil {
		panic(err)
	}

	// Delete events are not needed
	c.Source.WatchEtcdOps = append(c.Source.WatchEtcdOps, etcd.WithFilterDelete())

	// Setup context
	node.ctx = ctxattr.ContextWith(node.ctx, attribute.String("task", c.Name))

	return &orchestrator[T]{config: c, node: node, logger: node.logger}
}
