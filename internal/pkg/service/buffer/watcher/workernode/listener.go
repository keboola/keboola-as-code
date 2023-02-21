package workernode

import (
	"context"
	"sync"

	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// listeners are used to wait until all API nodes are synchronized to a requested revision.
type listeners struct {
	logger    log.Logger
	wg        *sync.WaitGroup
	lock      *sync.Mutex
	listeners map[listenerID]*Listener
}

type listenerID string

type Listener struct {
	all *listeners
	// id of the listener, so it can be removed from the map, if the requested revision is met
	id listenerID
	// rev is requested revision
	rev       int64
	cancelled bool
	// C channel is closed when all API nodes are synchronized to a requested revision
	C chan struct{}
}

func newListeners(logger log.Logger) *listeners {
	return &listeners{
		logger:    logger,
		wg:        &sync.WaitGroup{},
		lock:      &sync.Mutex{},
		listeners: make(map[listenerID]*Listener),
	}
}

func (v *Listener) Cancel() {
	v.all.lock.Lock()
	defer v.all.lock.Unlock()
	v.cancel()
}

func (v *Listener) cancel() {
	if !v.cancelled {
		v.cancelled = true
		close(v.C)
		delete(v.all.listeners, v.id)
		v.all.wg.Done()
	}
}

func (l *listeners) count() int {
	l.lock.Lock()
	defer l.lock.Unlock()
	return len(l.listeners)
}

// waitForRevision returns the channel that is closed when all API nodes are synced to the requested revision.
func (l *listeners) waitForRevision(requestedRev int64, currentRev *atomic.Int64) *Listener {
	l.lock.Lock()
	defer l.lock.Unlock()

	// Check if the condition has already been met
	if rev := currentRev.Load(); requestedRev <= rev || rev == noAPINode {
		ch := make(chan struct{})
		close(ch)
		return &Listener{all: l, cancelled: true, C: ch}
	}

	// Check again the next time, see onChange method
	out := &Listener{all: l, id: listenerID(idgenerator.Random(10)), rev: requestedRev, C: make(chan struct{})}
	l.wg.Add(1)
	l.listeners[out.id] = out
	return out
}

func (l *listeners) wait() {
	l.lock.Lock()
	count := len(l.listeners)
	l.lock.Unlock()

	if count > 0 {
		l.logger.Infof(`waiting for "%d" listeners`, count)
	}
	l.wg.Wait()
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
			break
		default:
			if v.rev <= rev || rev == noAPINode {
				v.cancel()
				unblockedCount++
			}
		}
	}

	return unblockedCount
}
