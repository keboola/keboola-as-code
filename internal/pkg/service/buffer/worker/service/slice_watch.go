package service

import (
	"context"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type activeSlicesWatcher struct {
	// slices contains keys of all slices in these states: opened, closing, uploading, failed
	// The slice key is removed when the uploaded state is reached.
	slices *prefixtree.AtomicTree[bool]

	lock      *sync.Mutex
	listeners map[listenerID]*listener
}

type listener struct {
	fileKey key.FileKey
	ch      chan struct{}
}

type listenerID string

func NewActiveSlicesWatcher(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, schema *schema.Schema, client *etcd.Client) (*activeSlicesWatcher, <-chan error) {
	w := &activeSlicesWatcher{
		slices:    prefixtree.New[bool](),
		lock:      &sync.Mutex{},
		listeners: make(map[listenerID]*listener),
	}

	// Watch for all active slices and build cache of all slices that have not yet been uploaded.
	// - Inset the slice key into AtomicTree if the state is: writing, closing, uploading, failed.
	// - Delete the slice key from the AtomicTree if the state is "uploaded".
	initDone := schema.Slices().AllActive().
		GetAllAndWatch(ctx, client, etcd.WithFilterDelete()).
		SetupConsumer(logger).
		WithForEach(func(events []etcdop.WatchEventT[model.Slice], header *op.Header, reset bool) {
			w.slices.Atomic(func(t *prefixtree.Tree[bool]) {
				if reset {
					t.Reset()
				}

				for _, event := range events {
					slice := event.Value
					switch slice.State {
					case slicestate.Writing, slicestate.Closing, slicestate.Uploading, slicestate.Failed:
						if event.Type == etcdop.CreateEvent {
							t.Insert(slice.SliceKey.String(), true)
						}
					case slicestate.Uploaded:
						if event.Type == etcdop.CreateEvent {
							t.Delete(slice.SliceKey.String())
						}
					default:
						panic(errors.Errorf(`unexpected state "%s"`, slice.State))
					}
				}
			})

			// Trigger change listeners
			if len(events) > 0 {
				w.lock.Lock()
				for id, l := range w.listeners {
					if w.countSlices(l.fileKey) == 0 {
						close(l.ch)
						delete(w.listeners, id)
					}
				}
				w.lock.Unlock()
			}
		}).
		StartConsumer(ctx, wg)

	return w, initDone
}

func (w *activeSlicesWatcher) WaitUntilAllSlicesUploaded(ctx context.Context, logger log.Logger, fileKey key.FileKey) error {
	// Is condition met?
	count := w.countSlices(fileKey)
	if count == 0 {
		return nil
	}

	if logger != nil {
		logger.InfofCtx(ctx, `waiting for "%d" slices to be uploaded`, count)
	}

	// Wait
	done, stop := w.onAllUploaded(fileKey)
	defer stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

// countSlices returns count of slices waiting for upload.
func (w *activeSlicesWatcher) countSlices(fileKey key.FileKey) int {
	count := 0
	w.slices.WalkPrefix(fileKey.String(), func(_ string, _ bool) bool {
		count++
		return false
	})
	return count
}

func (w *activeSlicesWatcher) onAllUploaded(fileKey key.FileKey) (ch <-chan struct{}, stopFn func()) {
	id := listenerID(idgenerator.Random(10))
	l := &listener{
		fileKey: fileKey,
		ch:      make(chan struct{}),
	}

	w.lock.Lock()
	w.listeners[id] = l
	w.lock.Unlock()

	return l.ch, func() {
		w.lock.Lock()
		delete(w.listeners, id)
		w.lock.Unlock()
	}
}
