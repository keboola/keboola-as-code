package distribution

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (n *Node) waitForSelfDiscovery(ctx context.Context, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error)

	// Waiting for self-discovery can be disabled in tests
	if n.config.selfDiscoveryTimeout == 0 {
		close(errCh)
		return errCh
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(errCh)

		l := n.OnChangeListener()
		defer l.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-n.clock.After(n.config.selfDiscoveryTimeout):
				errCh <- errors.New("self-discovery timeout")
				return
			case events := <-l.C:
				for _, event := range events {
					if event.Type == EventTypeAdd && event.NodeID == n.nodeID {
						return
					}
				}
			}
		}
	}()

	return errCh
}
