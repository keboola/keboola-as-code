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
	stream    *RestartableWatchStreamT[T]
	filter    func(t WatchEventT[T]) bool
	mapKey    func(kv *op.KeyValue, value T) string
	mapValue  func(kv *op.KeyValue, value T) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[V])

	tree         *prefixtree.AtomicTree[V]
	revisionLock *sync.Mutex
	revision     int64
}

type MirrorSetup[T any, V any] struct {
	stream    *RestartableWatchStreamT[T]
	filter    func(t WatchEventT[T]) bool
	mapKey    func(kv *op.KeyValue, value T) string
	mapValue  func(kv *op.KeyValue, value T) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[V])
}

type MirrorUpdate struct {
	Header  *Header
	Restart bool
}

type MirrorUpdateChanges[V any] struct {
	MirrorUpdate
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
	stream *RestartableWatchStreamT[T],
) MirrorSetup[T, T] {
	mapKey := func(kv *op.KeyValue, value T) string {
		return string(kv.Key)
	}
	mapValue := func(kv *op.KeyValue, value T) T {
		return value
	}
	return SetupMirror(stream, mapKey, mapValue)
}

func SetupMirror[T any, V any](
	stream *RestartableWatchStreamT[T],
	mapKey func(kv *op.KeyValue, value T) string,
	mapValue func(kv *op.KeyValue, value T) V,
) MirrorSetup[T, V] {
	return MirrorSetup[T, V]{
		stream:   stream,
		mapKey:   mapKey,
		mapValue: mapValue,
	}
}

func (s MirrorSetup[T, V]) Build() *Mirror[T, V] {
	return &Mirror[T, V]{
		stream:       s.stream,
		filter:       s.filter,
		mapKey:       s.mapKey,
		mapValue:     s.mapValue,
		tree:         prefixtree.New[V](),
		revisionLock: &sync.Mutex{},
		onUpdate:     s.onUpdate,
		onChanges:    s.onChanges,
	}
}

func (m *Mirror[T, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup, logger log.Logger) (initErr <-chan error) {
	_, errCh := m.stream.
		SetupConsumer().
		WithForEach(func(events []WatchEventT[T], header *Header, restart bool) {
			update := MirrorUpdate{Header: header, Restart: restart}
			changes := MirrorUpdateChanges[V]{MirrorUpdate: update}
			m.tree.Atomic(func(t *prefixtree.Tree[V]) {
				// Reset the tree after receiving the first batch after the restart.
				if restart {
					t.Reset()
				}

				// Atomically process all events
				for _, event := range events {
					if m.filter != nil && !m.filter(event) {
						continue
					}

					newKey := m.mapKey(event.Kv, event.Value)
					oldKey := newKey

					// Calculate oldKey based on the old value, if it is present.
					// It can be enabled by watch etcd.WithPrevKV() option.
					if event.PrevValue != nil {
						oldKey = m.mapKey(event.PrevKv, *event.PrevValue)
					}

					switch event.Type {
					case UpdateEvent:
						if event.PrevValue != nil {
							if oldKey != newKey {
								t.Delete(oldKey)
							}
						}
						newValue := m.mapValue(event.Kv, event.Value)
						if len(m.onChanges) > 0 {
							changes.Updated = append(changes.Updated, MirrorKVPair[V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case CreateEvent:
						newValue := m.mapValue(event.Kv, event.Value)
						if len(m.onChanges) > 0 {
							changes.Created = append(changes.Created, MirrorKVPair[V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case DeleteEvent:
						if len(m.onChanges) > 0 {
							oldValue, _ := t.Get(oldKey)
							changes.Deleted = append(changes.Deleted, MirrorKVPair[V]{Key: oldKey, Value: oldValue})
						}
						t.Delete(oldKey)
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}

				// Store the last synced revision
				m.revisionLock.Lock()
				m.revision = header.Revision
				m.revisionLock.Unlock()
				logger.Debugf(ctx, `synced to revision %d`, header.Revision)
			})

			// Call callbacks
			for _, fn := range m.onUpdate {
				go fn(update)
			}
			for _, fn := range m.onChanges {
				go fn(changes)
			}
		}).
		StartConsumer(ctx, wg, logger)
	return errCh
}

// WithFilter set a filter, the filter must return true if the event should be processed.
func (s MirrorSetup[T, V]) WithFilter(fn func(event WatchEventT[T]) bool) MirrorSetup[T, V] {
	s.filter = fn
	return s
}

// WithOnUpdate callback is triggered on each atomic tree update.
// The update argument contains actual etcd revision,
// on which the mirror is synchronized.
func (s MirrorSetup[T, V]) WithOnUpdate(fn func(update MirrorUpdate)) MirrorSetup[T, V] {
	s.onUpdate = append(s.onUpdate, fn)
	return s
}

// WithOnChanges callback is triggered on each atomic tree update.
// The changes argument contains actual etcd revision,
// on which the mirror is synchronized and also a list of changes.
func (s MirrorSetup[T, V]) WithOnChanges(fn func(changes MirrorUpdateChanges[V])) MirrorSetup[T, V] {
	s.onChanges = append(s.onChanges, fn)
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
