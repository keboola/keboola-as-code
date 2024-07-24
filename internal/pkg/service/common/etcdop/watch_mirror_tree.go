package etcdop

import (
	"context"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MirrorTree [T,V] is an in memory AtomicTree filled via the etcd Watch API from a RestartableWatchStream[T].
// Tree read operations are publicly available, writing is performed exclusively from the watch stream.
// Key (string) and value (V) are generated from incoming WatchEvent[T] by custom callbacks, see MirrorTreeSetup.
// Start with SetupMirrorTree function.
//
// MirrorTree is ideal to get all keys from a common prefix.
// If you need only quick single key access or iteration over all keys, use MirrorMap.
//
// MirrorTree map is a little slower, but you can scan a prefix without full scan.
type MirrorTree[T any, V any] struct {
	stream    RestartableWatchStream[T]
	filter    func(event WatchEvent[T]) bool
	mapKey    func(key string, value T) string
	mapValue  func(key string, value T) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[string, V])

	tree         *prefixtree.AtomicTree[V]
	revisionLock sync.RWMutex
	revision     int64
}

type MirrorTreeSetup[T any, V any] struct {
	stream    RestartableWatchStream[T]
	filter    func(event WatchEvent[T]) bool
	mapKey    func(key string, value T) string
	mapValue  func(key string, value T) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[string, V])
}

// SetupFullMirrorTree - without key and value mapping.
func SetupFullMirrorTree[T any](
	stream *RestartableWatchStreamT[T],
) MirrorTreeSetup[T, T] {
	mapKey := func(key string, value T) string {
		return key
	}
	mapValue := func(key string, value T) T {
		return value
	}
	return SetupMirrorTree[T](stream, mapKey, mapValue)
}

func SetupMirrorTree[T any, V any](
	stream RestartableWatchStream[T],
	mapKey func(key string, value T) string,
	mapValue func(key string, value T) V,
) MirrorTreeSetup[T, V] {
	return MirrorTreeSetup[T, V]{
		stream:   stream,
		mapKey:   mapKey,
		mapValue: mapValue,
	}
}

func (s MirrorTreeSetup[T, V]) BuildMirror() *MirrorTree[T, V] {
	return &MirrorTree[T, V]{
		stream:    s.stream,
		filter:    s.filter,
		mapKey:    s.mapKey,
		mapValue:  s.mapValue,
		tree:      prefixtree.New[V](),
		onUpdate:  s.onUpdate,
		onChanges: s.onChanges,
	}
}

func (m *MirrorTree[T, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup, logger log.Logger) (initErr <-chan error) {
	consumer := newConsumerSetup(m.stream).
		WithForEach(func(events []WatchEvent[T], header *Header, restart bool) {
			update := MirrorUpdate{Header: header, Restart: restart}
			changes := MirrorUpdateChanges[string, V]{MirrorUpdate: update}
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

					newKey := m.mapKey(event.Key, event.Value)
					oldKey := newKey

					// Calculate oldKey based on the old value, if it is present.
					// It can be enabled by watch etcd.WithPrevKV() option.
					if event.PrevValue != nil {
						oldKey = m.mapKey(event.Key, *event.PrevValue)
					}

					switch event.Type {
					case UpdateEvent:
						if event.PrevValue != nil {
							if oldKey != newKey {
								t.Delete(oldKey)
							}
						}
						newValue := m.mapValue(event.Key, event.Value)
						if len(m.onChanges) > 0 {
							changes.Updated = append(changes.Updated, MirrorKVPair[string, V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case CreateEvent:
						newValue := m.mapValue(event.Key, event.Value)
						if len(m.onChanges) > 0 {
							changes.Created = append(changes.Created, MirrorKVPair[string, V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case DeleteEvent:
						if len(m.onChanges) > 0 {
							oldValue, _ := t.Get(oldKey)
							changes.Deleted = append(changes.Deleted, MirrorKVPair[string, V]{Key: oldKey, Value: oldValue})
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
		BuildConsumer()

	return consumer.StartConsumer(ctx, wg, logger)
}

// WithFilter set a filter, the filter must return true if the event should be processed.
func (s MirrorTreeSetup[T, V]) WithFilter(fn func(event WatchEvent[T]) bool) MirrorTreeSetup[T, V] {
	s.filter = fn
	return s
}

// WithOnUpdate callback is triggered on each atomic tree update.
// The update argument contains actual etcd revision,
// on which the mirror is synchronized.
func (s MirrorTreeSetup[T, V]) WithOnUpdate(fn func(update MirrorUpdate)) MirrorTreeSetup[T, V] {
	s.onUpdate = append(s.onUpdate, fn)
	return s
}

// WithOnChanges callback is triggered on each atomic tree update.
// The changes argument contains actual etcd revision,
// on which the mirror is synchronized and also a list of changes.
func (s MirrorTreeSetup[T, V]) WithOnChanges(fn func(changes MirrorUpdateChanges[string, V])) MirrorTreeSetup[T, V] {
	s.onChanges = append(s.onChanges, fn)
	return s
}

func (m *MirrorTree[T, V]) Restart(cause error) {
	m.stream.Restart(cause)
}

func (m *MirrorTree[T, V]) Revision() int64 {
	m.revisionLock.RLock()
	defer m.revisionLock.RUnlock()
	return m.revision
}

func (m *MirrorTree[T, V]) Atomic(do func(t prefixtree.TreeReadOnly[V])) {
	m.tree.AtomicReadOnly(do)
}

func (m *MirrorTree[T, V]) Get(key string) (V, bool) {
	return m.tree.Get(key)
}

func (m *MirrorTree[T, V]) All() []V {
	return m.tree.All()
}

func (m *MirrorTree[T, V]) AllFromPrefix(key string) []V {
	return m.tree.AllFromPrefix(key)
}

func (m *MirrorTree[T, V]) FirstFromPrefix(key string) (value V, found bool) {
	return m.tree.FirstFromPrefix(key)
}

func (m *MirrorTree[T, V]) LastFromPrefix(key string) (value V, found bool) {
	return m.tree.LastFromPrefix(key)
}

func (m *MirrorTree[T, V]) WalkPrefix(key string, fn func(key string, value V) (stop bool)) {
	m.tree.WalkPrefix(key, fn)
}

func (m *MirrorTree[T, V]) WalkAll(fn func(key string, value V) (stop bool)) {
	m.tree.WalkAll(fn)
}

func (m *MirrorTree[T, V]) ToMap() map[string]V {
	return m.tree.ToMap()
}
