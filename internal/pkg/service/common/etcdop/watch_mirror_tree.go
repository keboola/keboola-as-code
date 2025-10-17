package etcdop

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/memory"
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
	mapValue  func(key string, value T, rawValue *op.KeyValue, oldValue *V) V
	onUpdate  []func(update MirrorUpdate)
	onChanges []func(changes MirrorUpdateChanges[string, V])

	updatedLock sync.RWMutex
	updated     chan struct{}

	tree         *prefixtree.AtomicTree[V]
	revisionLock sync.RWMutex
	revision     int64
}

type MirrorTreeSetup[T any, V any] struct {
	stream    RestartableWatchStream[T]
	filter    func(event WatchEvent[T]) bool
	mapKey    func(key string, value T) string
	mapValue  func(key string, value T, rawValue *op.KeyValue, oldValue *V) V
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
	mapValue := func(key string, value T, _ *op.KeyValue, _ *T) T {
		return value
	}
	return SetupMirrorTree[T](stream, mapKey, mapValue)
}

func SetupMirrorTree[T any, V any](
	stream RestartableWatchStream[T],
	mapKey func(key string, value T) string,
	mapValue func(key string, value T, rawValue *op.KeyValue, oldValue *V) V, // oldValue is set only on the update event
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
		updated:   make(chan struct{}),
	}
}

func (m *MirrorTree[T, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, tel telemetry.Telemetry, watchTelemetryInterval time.Duration) (initErr <-chan error) {
	ctx = ctxattr.ContextWith(ctx, attribute.String("stream.prefix", m.stream.WatchedPrefix()))

	wg.Add(1)
	// Launching a goroutine to start collecting telemetry data for the MirrorTree.
	// This allows asynchronous monitoring of metrics related to the tree's performance and usage.
	go m.startTelemetryCollection(ctx, wg, tel, logger, watchTelemetryInterval)

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
						if oldKey != newKey {
							t.Delete(oldKey)
						}

						var oldValuePtr *V
						if oldValue, found := t.Get(oldKey); found {
							oldValuePtr = &oldValue
						}

						newValue := m.mapValue(event.Key, event.Value, event.Kv, oldValuePtr)

						if len(m.onChanges) > 0 {
							changes.Updated = append(changes.Updated, MirrorKVPair[string, V]{Key: newKey, Value: newValue})
						}
						t.Insert(newKey, newValue)
					case CreateEvent:
						newValue := m.mapValue(event.Key, event.Value, event.Kv, nil)
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

				logger.Debugf(ctx, `watch stream mirror synced to revision %d`, header.Revision)

				// Unblock WaitForRevision loops
				m.updatedLock.Lock()
				close(m.updated)
				m.updated = make(chan struct{})
				m.updatedLock.Unlock()
			})

			m.recordTelemetry(ctx, tel)
			m.recordMemoryTelemetry(ctx, tel)

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

func (m *MirrorTree[T, V]) WaitForRevision(ctx context.Context, expected int64) error {
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
			// The revision has been updated, try again
		}
	}
}

func (m *MirrorTree[T, V]) Atomic(do func(t prefixtree.TreeReadOnly[V])) {
	m.tree.AtomicReadOnly(do)
}

func (m *MirrorTree[T, V]) Get(key string) (V, bool) {
	return m.tree.Get(key)
}

func (m *MirrorTree[T, V]) Len() int {
	return m.tree.Len()
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

// recordTelemetry captures and reports the number of keys in the MirrorTree using the provided telemetry system.
func (m *MirrorTree[T, V]) recordTelemetry(ctx context.Context, tel telemetry.Telemetry) {
	tel.Meter().
		IntCounter(
			"keboola.go.mirror.tree.num.keys",
			"Number of keys in the mirror tree.",
			"count",
		).
		Add(
			ctx,
			int64(m.tree.Len()),
			metric.WithAttributes(attribute.String("prefix", m.stream.WatchedPrefix())))
}

func (m *MirrorTree[T, V]) recordMemoryTelemetry(ctx context.Context, tel telemetry.Telemetry) {
	// Initialize a variable to track memory allocated for the tree
	var memoryConsumed int64

	// Measure base memory usage of the tree structure
	memoryConsumed += int64(unsafe.Sizeof(*m))

	// Measure size of the tree nodes and their elements
	m.tree.AtomicReadOnly(func(t prefixtree.TreeReadOnly[V]) {
		t.WalkAll(func(key string, value V) (stop bool) {
			memoryConsumed += int64(len(key))           // Account for key size
			memoryConsumed += int64(memory.Size(value)) // Account for value size
			return false
		})
	})

	// Emit telemetry
	meter := tel.Meter()

	// Gauge for tree memory consumption
	meter.IntCounter(
		"keboola.go.mirror_tree.memory.usage.bytes",
		"Memory consumed by the MirrorTree, including keys and values.",
		"bytes",
	).Add(ctx, memoryConsumed, metric.WithAttributes(attribute.String("prefix", m.stream.WatchedPrefix())))
}

// startTelemetryCollection begins periodic telemetry reporting for the MirrorTree, including memory usage and key count.
// It runs until the given context is canceled and ensures the provided wait group is marked as done upon completion.
func (m *MirrorTree[T, V]) startTelemetryCollection(ctx context.Context, wg *sync.WaitGroup, tel telemetry.Telemetry, log log.Logger, watchTelemetryInterval time.Duration) {
	defer wg.Done()

	ticker := time.NewTicker(watchTelemetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.recordTelemetry(ctx, tel)
			m.recordMemoryTelemetry(ctx, tel)
		case <-ctx.Done():
			log.Debugf(ctx, "Telemetry collection for tree stopped: %v", ctx.Err())
			return
		}
	}
}
