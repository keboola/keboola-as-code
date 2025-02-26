package closesync

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SourceNode struct {
	logger log.Logger
	client *etcd.Client
	sess   *etcdop.Session
	nodeID string
	schema schema
}

func NewSourceNode(d dependencies, nodeID string) (*SourceNode, error) {
	n := &SourceNode{
		client: d.EtcdClient(),
		logger: d.Logger().WithComponent("close-sync.source"),
		nodeID: nodeID,
		schema: newSchema(d.EtcdSerde()),
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		n.logger.Infof(ctx, "closing close-sync source node")
		cancel(errors.New("shutting down: close-sync source node"))
		wg.Wait()
		n.logger.Infof(ctx, "closed close-sync source node")
	})

	// Start concurrent session with retries
	{
		var errCh <-chan error
		n.sess, errCh = etcdop.NewSessionBuilder().Start(ctx, wg, n.logger, n.client)
		if err := <-errCh; err != nil {
			return nil, err
		}
	}

	if err := n.Notify(ctx, 0); err != nil {
		return nil, err
	}

	return n, nil
}

// Notify - the source node notifies that all changes up to the reported revision has been processed.
// That means, all pipelines matching slices closed up to the revision, are already closed too.
func (n *SourceNode) Notify(ctx context.Context, rev int64) error {
	sess, err := n.sess.Session()
	if err != nil {
		return err
	}

	// Update key in the database, with the current revision.
	// The key contains lease, so if the source node unexpectedly gone, the key is automatically removed, after TTL seconds.
	return n.schema.SourceNode(n.nodeID).
		Put(n.client, rev, etcd.WithLease(sess.Lease())).
		Do(ctx).
		Err()
}
