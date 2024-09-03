package pipeline

import "sync"

// readyNotifier block SinkPipeline opening until at least one SlicePipeline is ready.
type readyNotifier struct {
	lock  sync.Mutex
	ready chan struct{}
}

func newReadyNotifier() *readyNotifier {
	return &readyNotifier{ready: make(chan struct{})}
}

func (n *readyNotifier) NotifyReady() {
	n.lock.Lock()
	defer n.lock.Unlock()
	select {
	case <-n.ready:
	default:
		close(n.ready)
	}
}

func (n *readyNotifier) WaitCh() <-chan struct{} {
	return n.ready
}
