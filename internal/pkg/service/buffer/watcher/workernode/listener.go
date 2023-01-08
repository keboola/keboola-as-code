package workernode

import (
	"context"
	"sync"

	gonanoid "github.com/matoous/go-nanoid/v2"
	"go.uber.org/atomic"
)

// listeners are used to wait until all API nodes are synchronized to a requested revision.
type listeners struct {
	lock      *sync.Mutex
	listeners map[listenerID]*listener
}

type listenerID string

type listener struct {
	// id of the listener, so it can be removed from the map, if the requested revision is met
	id listenerID
	// rev is requested revision
	rev int64
	// C channel is closed when all API nodes are synchronized to a requested revision
	C chan struct{}
}

func newListeners() *listeners {
	return &listeners{
		lock:      &sync.Mutex{},
		listeners: make(map[listenerID]*listener),
	}
}

// waitForRevision returns the channel that is closed when all API nodes are synced to the requested revision.
func (l *listeners) waitForRevision(requestedRev int64, currentRev *atomic.Int64) <-chan struct{} {
	out := &listener{id: listenerID(gonanoid.Must(10)), rev: requestedRev, C: make(chan struct{})}

	l.lock.Lock()
	defer l.lock.Unlock()

	if out.rev <= currentRev.Load() {
		// The condition has already been met
		close(out.C)
	} else {
		// Check again the next time, see onChange method
		l.listeners[out.id] = out
	}

	return out.C
}

// onChange is called when the minimal revision, that match all API nodes, is increased.
func (l *listeners) onChange(ctx context.Context, currentRev *atomic.Int64) int {
	l.lock.Lock()
	defer l.lock.Unlock()

	rev := currentRev.Load()
	unblockedCount := 0
	for _, v := range l.listeners {
		v := v
		select {
		case <-ctx.Done():
			return 0
		default:
			if v.rev <= rev {
				close(v.C)
				delete(l.listeners, v.id)
				unblockedCount++
			}
		}
	}

	return unblockedCount
}
