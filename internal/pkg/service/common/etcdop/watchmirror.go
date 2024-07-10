package etcdop

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Mirror [T,V] is an in memory AtomicTree filled via the etcd Watch API from a WatchStreamT[T].
// Tree read operations are publicly available, writing is performed exclusively from the watch stream.
// Key (string) and value (V) are generated from incoming WatchEventT by custom callbacks, see MirrorSetup.
// Start with SetupMirror function.
type Mirror[T any, V any] struct {
	stream       *RestartableWatchStreamT[T]
	tree         *prefixtree.AtomicTree[V]
	revisionLock *sync.Mutex
	revision     int64
	onUpdate     []func(updateLog MirrorUpdatedKeys[V])
}

type MirrorSetup[T any, V any] struct {
	logger   log.Logger
	stream   *RestartableWatchStreamT[T]
	filter   func(t WatchEventT[T]) bool
	mapKey   func(kv *op.KeyValue, value T) string
	mapValue func(kv *op.KeyValue, value T) V
	onUpdate []func(updateLog MirrorUpdatedKeys[V])
}

type MirrorUpdatedKeys[V any] struct {
	Created []MirrorKVPair[V]
	Updated []MirrorKVPair[V]
	Deleted []MirrorKVPair[V]
}

type MirrorKVPair[V any] struct {
	Key   string
	Value V
}

// SetupFullMirror - without key and value mapping.
func SetupFullMirror[T any](
	logger log.Logger,
	stream *RestartableWatchStreamT[T],
) MirrorSetup[T, T] {
	mapKey := func(kv *op.KeyValue, value T) string {
		return string(kv.Key)
	}
	mapValue := func(kv *op.KeyValue, value T) T {
		return value
	}
	return MirrorSetup[T, T]{
		logger:   logger,
		stream:   stream,
		mapKey:   mapKey,
		mapValue: mapValue,
	}
}

func SetupMirror[T any, V any](
	logger log.Logger,
	stream *RestartableWatchStreamT[T],
	mapKey func(kv *op.KeyValue, value T) string,
	mapValue func(kv *op.KeyValue, value T) V,
) MirrorSetup[T, V] {
	return MirrorSetup[T, V]{
		logger:   logger,
		stream:   stream,
		mapKey:   mapKey,
		mapValue: mapValue,
	}
}

func (s MirrorSetup[T, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup) (mirror *Mirror[T, V], initErr <-chan error) {
	mirror = &Mirror[T, V]{
		stream:       s.stream,
		tree:         prefixtree.New[V](),
		revisionLock: &sync.Mutex{},
		onUpdate:     s.onUpdate,
	}
	errCh := s.stream.
		SetupConsumer(s.logger).
		WithForEach(func(events []WatchEventT[T], header *Header, restart bool) {
			changes := MirrorUpdatedKeys[V]{}
			mirror.tree.Atomic(func(t *prefixtree.Tree[V]) {
				// Reset the tree after receiving the first batch after the restart.
				if restart {
					t.Reset()
				}

				// Atomically process all events
				for _, event := range events {
					if s.filter != nil && !s.filter(event) {
						continue
					}

					newKey := s.mapKey(event.Kv, event.Value)
					oldKey := newKey

					// Calculate oldKey based on the old value, if it is present.
					// It can be enabled by watch clientv3.WithPrevKV() option.
					if event.PrevValue != nil {
						oldKey = s.mapKey(event.PrevKv, *event.PrevValue)
					}

					switch event.Type {
					case UpdateEvent:
						if event.PrevValue != nil {
							if oldKey != newKey {
								t.Delete(oldKey)
							}
						}
						newValue := s.mapValue(event.Kv, event.Value)
						if len(mirror.onUpdate) > 0 {
							changes.Updated = append(changes.Updated, MirrorKVPair[V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case CreateEvent:
						newValue := s.mapValue(event.Kv, event.Value)
						if len(mirror.onUpdate) > 0 {
							changes.Created = append(changes.Created, MirrorKVPair[V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case DeleteEvent:
						if len(mirror.onUpdate) > 0 {
							oldValue, _ := t.Get(oldKey)
							changes.Deleted = append(changes.Deleted, MirrorKVPair[V]{Key: oldKey, Value: oldValue})
						}
						t.Delete(oldKey)
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}

				// Store the last synced revision
				mirror.revisionLock.Lock()
				mirror.revision = header.Revision
				mirror.revisionLock.Unlock()
				s.logger.Debugf(ctx, `synced to revision %d`, header.Revision)
			})

			// Call OnUpdate callbacks
			for _, fn := range mirror.onUpdate {
				go fn(changes)
			}
		}).
		StartConsumer(ctx, wg)
	return mirror, errCh
}

// WithFilter set a filter, the filter must return true if the event should be processed.
func (s MirrorSetup[T, V]) WithFilter(fn func(event WatchEventT[T]) bool) MirrorSetup[T, V] {
	s.filter = fn
	return s
}

func (s MirrorSetup[T, V]) WithOnUpdate(fn func(changes MirrorUpdatedKeys[V])) MirrorSetup[T, V] {
	s.onUpdate = append(s.onUpdate, fn)
	return s
}

func (m *Mirror[T, V]) Restart(cause error) {
	m.stream.Restart(cause)
}

func (m *Mirror[T, V]) Revision() int64 {
	m.revisionLock.Lock()
	defer m.revisionLock.Unlock()
	return m.revision
}

func (m *Mirror[T, V]) Atomic(do func(t prefixtree.TreeReadOnly[V])) {
	m.tree.AtomicReadOnly(do)
}

func (m *Mirror[T, V]) Get(key string) (V, bool) {
	return m.tree.Get(key)
}

func (m *Mirror[T, V]) All() []V {
	return m.tree.All()
}

func (m *Mirror[T, V]) AllFromPrefix(key string) []V {
	return m.tree.AllFromPrefix(key)
}

func (m *Mirror[T, V]) FirstFromPrefix(key string) (value V, found bool) {
	return m.tree.FirstFromPrefix(key)
}

func (m *Mirror[T, V]) LastFromPrefix(key string) (value V, found bool) {
	return m.tree.LastFromPrefix(key)
}

func (m *Mirror[T, V]) WalkPrefix(key string, fn func(key string, value V) (stop bool)) {
	m.tree.WalkPrefix(key, fn)
}

func (m *Mirror[T, V]) WalkAll(fn func(key string, value V) (stop bool)) {
	m.tree.WalkAll(fn)
}

func (m *Mirror[T, V]) ToMap() map[string]V {
	return m.tree.ToMap()
}
