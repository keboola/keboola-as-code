// Package apinode provides a configuration cache of receivers and slices for an API node.
// This speeds up the import endpoint, since no query to the etcd is needed.
// See documentation in the "watcher" package.
package apinode

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/watcher/apinode/revision"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

// APINode contains cached state of all configuration objects required by the import endpoint.
type APINode struct {
	logger log.Logger
	state  *state
}

// ReceiverCore is simplified version of the receiver, it contains only the data required by the import endpoint.
type ReceiverCore struct {
	model.ReceiverBase
	Slices []model.Slice
}

type Dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func NewAPINode(d Dependencies) (*APINode, error) {
	n := &APINode{
		logger: d.Logger().AddPrefix("[watcher][api]"),
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

	// Create cached state synchronized by the Watch API
	if v, err := newState(ctx, wg, d); err == nil {
		n.state = v
	} else {
		return nil, err
	}

	return n, nil
}

// GetReceiver from the cache.
// UnlockFn must be called when the work, based on the returned ReceiverCore, is completed.
func (n *APINode) GetReceiver(receiverKey key.ReceiverKey) (out ReceiverCore, found bool, unlockFn revision.UnlockFn) {
	return n.state.GetReceiver(receiverKey)
}
