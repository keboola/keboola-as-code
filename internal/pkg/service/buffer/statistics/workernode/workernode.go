package workernode

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type Node struct {
	logger log.Logger
	clock  clock.Clock
	client *etcd.Client
	schema *schema.Schema
}

type Dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
}

func New(d Dependencies) (*Node, error) {
	// Create
	n := &Node{
		logger: d.Logger().AddPrefix("[stats][worker]"),
		clock:  d.Clock(),
		client: d.EtcdClient(),
		schema: d.Schema(),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		n.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		n.logger.Info("shutdown done")
	})

	<-n.watch(ctx, wg)

	return n, nil
}

func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	// pfx := n.schema.Slices().Active()
	// ch := pfx.GetAllAndWatch(ctx, n.client)
	return nil
}
