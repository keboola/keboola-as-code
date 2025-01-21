package etcdop

import (
	"context"
	"maps"
	"sync"

	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MirrorMap [T,K, V] is an in memory Go map filled via the etcd Watch API from a RestartableWatchStream[T].
// Map read operations are publicly available, writing is performed exclusively from the watch stream.
// Key (K) and value (V) are generated from incoming WatchEvent[T] by custom callbacks, see MirrorMapSetup.
// Start with SetupMirrorMap function.
//
// MirrorMap is ideal for quick single key access or iteration over all keys.
// To get all keys from a common prefix, use MirrorTree instead.
type MirrorMap[T any, K comparable, V any] struct {
	stream    RestartableWatchStream[T]
	filter    func(event WatchEvent[T]) bool
	mapKey    func(key string, value T) K
	mapValue  func(key string, value T, rawValue *op.KeyValue, oldValue *V) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[K, V])

	updatedLock sync.RWMutex
	updated     chan struct{}

	mapLock      sync.RWMutex
	mapData      map[K]V
	revisionLock sync.RWMutex
	revision     int64
}

type MirrorMapSetup[T any, K comparable, V any] struct {
	stream    RestartableWatchStream[T]
	filter    func(event WatchEvent[T]) bool
	mapKey    func(key string, value T) K
	mapValue  func(key string, value T, rawValue *op.KeyValue, oldValue *V) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[K, V])
}

func SetupMirrorMap[T any, K comparable, V any](
	stream RestartableWatchStream[T],
	mapKey func(key string, value T) K,
	mapValue func(key string, value T, rawValue *op.KeyValue, oldValue *V) V, // oldValue is set only on the update event
) MirrorMapSetup[T, K, V] {
	return MirrorMapSetup[T, K, V]{
		stream:   stream,
		mapKey:   mapKey,
		mapValue: mapValue,
	}
}

func (s MirrorMapSetup[T, K, V]) BuildMirror() *MirrorMap[T, K, V] {
	return &MirrorMap[T, K, V]{
		stream:    s.stream,
		filter:    s.filter,
		mapKey:    s.mapKey,
		mapValue:  s.mapValue,
		mapData:   make(map[K]V),
		onUpdate:  s.onUpdate,
		onChanges: s.onChanges,
		updated:   make(chan struct{}),
	}
}

func (m *MirrorMap[T, K, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup, logger log.Logger) (initErr <-chan error) {
	ctx = ctxattr.ContextWith(ctx, attribute.String("stream.prefix", m.stream.WatchedPrefix()))

	consumer := newConsumerSetup(m.stream).
		WithForEach(func(events []WatchEvent[T], header *Header, restart bool) {
			update := MirrorUpdate{Header: header, Restart: restart}
			changes := MirrorUpdateChanges[K, V]{MirrorUpdate: update}

			m.mapLock.Lock()

			// Reset the tree after receiving the first batch after the restart.
			if restart {
				m.mapData = make(map[K]V)
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
					if oldKey != newKey {
						delete(m.mapData, oldKey)
					}

					var oldValuePtr *V
					if oldValue, found := m.mapData[oldKey]; found {
						oldValuePtr = &oldValue
					}

					newValue := m.mapValue(event.Key, event.Value, event.Kv, oldValuePtr)

					if len(m.onChanges) > 0 {
						changes.Updated = append(changes.Updated, MirrorKVPair[K, V]{Key: newKey, Value: newValue})
					}
					m.mapData[newKey] = newValue
				case CreateEvent:
					newValue := m.mapValue(event.Key, event.Value, event.Kv, nil)
					if len(m.onChanges) > 0 {
						changes.Created = append(changes.Created, MirrorKVPair[K, V]{Key: newKey, Value: newValue})
					}
					m.mapData[newKey] = newValue
				case DeleteEvent:
					if len(m.onChanges) > 0 {
						oldValue := m.mapData[oldKey]
						changes.Deleted = append(changes.Deleted, MirrorKVPair[K, V]{Key: oldKey, Value: oldValue})
					}
					delete(m.mapData, oldKey)
				default:
					panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
				}
			}

			m.mapLock.Unlock()

			// Store the last synced revision
			m.revisionLock.Lock()
			m.revision = header.Revision
			m.revisionLock.Unlock()

			// TODO: add logs for each revision
			// TODO: maybe we missed the revision, but after deadline?
			// TODO: the statistics for slice does not exists
			// TODO: slicerotation issue -> old file has not been imported
			// TODO: when all slices are not in `Uploaded` state -> file import not working
			logger.Debugf(ctx, `watch stream mirror synced to revision %d`, header.Revision)

			// Unblock WaitForRevision loops
			m.updatedLock.Lock()
			close(m.updated)
			m.updated = make(chan struct{})
			m.updatedLock.Unlock()

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
func (s MirrorMapSetup[T, K, V]) WithFilter(fn func(event WatchEvent[T]) bool) MirrorMapSetup[T, K, V] {
	s.filter = fn
	return s
}

// WithOnUpdate callback is triggered on each atomic tree update.
// The update argument contains actual etcd revision,
// on which the mirror is synchronized.
func (s MirrorMapSetup[T, K, V]) WithOnUpdate(fn func(update MirrorUpdate)) MirrorMapSetup[T, K, V] {
	s.onUpdate = append(s.onUpdate, fn)
	return s
}

// WithOnChanges callback is triggered on each atomic tree update.
// The changes argument contains actual etcd revision,
// on which the mirror is synchronized and also a list of changes.
func (s MirrorMapSetup[T, K, V]) WithOnChanges(fn func(changes MirrorUpdateChanges[K, V])) MirrorMapSetup[T, K, V] {
	s.onChanges = append(s.onChanges, fn)
	return s
}

func (m *MirrorMap[T, K, V]) Restart(cause error) {
	m.stream.Restart(cause)
}

func (m *MirrorMap[T, K, V]) Revision() int64 {
	m.revisionLock.RLock()
	defer m.revisionLock.RUnlock()
	return m.revision
}

func (m *MirrorMap[T, K, V]) WaitForRevision(ctx context.Context, expected int64) error {
	for {
		m.revisionLock.RLock()
		actual := m.revision
		m.revisionLock.RUnlock()

		// Is the condition already met?
		if actual >= expected {
			return nil
		}

		// Get update notifier
		m.updatedLock.RLock()
		notifier := m.updated
		m.updatedLock.RUnlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notifier:
			// try again
		}
	}
}

func (m *MirrorMap[T, K, V]) Len() int {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	return len(m.mapData)
}

func (m *MirrorMap[T, K, V]) Get(key K) (V, bool) {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	v, ok := m.mapData[key]
	return v, ok
}

func (m *MirrorMap[T, K, V]) CloneMap() map[K]V {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	return maps.Clone(m.mapData)
}

func (m *MirrorMap[T, K, V]) ForEach(fn func(K, V) (stop bool)) {
	m.mapLock.RLock()
	defer m.mapLock.RUnlock()
	for k, v := range m.mapData {
		if fn(k, v) {
			return
		}
	}
}
