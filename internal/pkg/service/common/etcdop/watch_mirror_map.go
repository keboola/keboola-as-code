package etcdop

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
	"unsafe"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/memory"
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

// StartMirroring initializes the mirroring process for the MirrorMap by starting a watcher and processing events.
// It locks and updates the internal map on event changes, captures telemetry, and invokes registered callbacks.
// Returns a channel of initialization errors if the consumer fails to start.
func (m *MirrorMap[T, K, V]) StartMirroring(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, tel telemetry.Telemetry, watchTelemetryInterval time.Duration) (initErr <-chan error) {
	ctx = ctxattr.ContextWith(ctx, attribute.String("stream.prefix", m.stream.WatchedPrefix()))

	// Start telemetry collection in a separate goroutine.
	// This routine collects metrics about the memory usage and the state of the MirrorMap.
	wg.Add(1)
	go m.startTelemetryCollection(ctx, wg, logger, tel, watchTelemetryInterval)

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

			fmt.Println("revision in mirrormap synced", header.Revision)
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
		fmt.Println("waiting for revision in mirrormap", m.revision, expected)
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

func (m *MirrorMap[T, K, V]) recordTelemetry(ctx context.Context, tel telemetry.Telemetry) {
	tel.Meter().
		IntCounter(
			"keboola.go.mirror.map.num.keys",
			"Number of keys in the mirror map.",
			"count",
		).
		Add(
			ctx,
			int64(len(m.mapData)),
			metric.WithAttributes(attribute.String("prefix", m.stream.WatchedPrefix())),
		)
}

func (m *MirrorMap[T, K, V]) recordMemoryTelemetry(ctx context.Context, tel telemetry.Telemetry) {
	// Variable to track memory consumed
	var memoryConsumed int64

	// Measure base memory usage of the map structure
	memoryConsumed += int64(unsafe.Sizeof(*m))

	// Lock the map to measure memory usage of its elements
	for k, v := range m.mapData {
		memoryConsumed += int64(unsafe.Sizeof(k)) // Add key size
		memoryConsumed += int64(memory.Size(v))   // Add value size
	}

	// Emit metrics for the current map memory
	meter := tel.Meter()

	// Track memory consumed by the map itself
	meter.IntCounter(
		"keboola.go.mirror.map.memory.usage.bytes",
		"Memory consumed by the MirrorMap, including keys and values.",
		"bytes",
	).Add(ctx, memoryConsumed, metric.WithAttributes(attribute.String("prefix", m.stream.WatchedPrefix())))
}

// Function for periodic telemetry collection (runs as a goroutine).
func (m *MirrorMap[T, K, V]) startTelemetryCollection(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, tel telemetry.Telemetry, watchTelemetryInterval time.Duration) {
	defer wg.Done()

	ticker := time.NewTicker(watchTelemetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Emit telemetry metrics
			m.mapLock.RLock()
			m.recordTelemetry(ctx, tel)
			m.recordMemoryTelemetry(ctx, tel)
			m.mapLock.RUnlock()
		case <-ctx.Done():
			logger.Debugf(ctx, "Telemetry collection stopped: %v", ctx.Err())
			return
		}
	}
}
