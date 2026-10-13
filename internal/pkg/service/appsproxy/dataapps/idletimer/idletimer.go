package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// k8sWorkloads is the part of the K8s state watcher this loop needs.
type k8sWorkloads interface {
	HasSynced() bool
	ScanForSleepCandidates() k8sapp.SleepScan
	Sleep(ctx context.Context, ref k8sapp.WorkloadRef, expectMember string) (bool, error)
}

type Manager struct {
	// wg counts marks that have been accepted but not yet handled, so a waiter
	// covers the write a mark goes on to make.
	wg sync.WaitGroup
	// A websocket frame can arrive after Shutdown begins: hijacked connections
	// are not drained by the HTTP server. Admission closes under this lock, so
	// no nudge is ever accepted after Wait has started counting down.
	closeLock sync.RWMutex
	closed    bool
	// stopping ends the writers, and is closed only once every accepted nudge
	// has been handled.
	stopping chan struct{}

	suspendEnabled bool
	clock          clockwork.Clock
	logger         log.Logger
	client         *client
	workloads      k8sWorkloads
	records        *recordCache
	metrics        *metrics
	stateMap       *syncmap.SyncMap[k8sapp.WorkloadRef, state]

	// These are touched only by the tick goroutine.
	warnedUnresolved   map[api.AppID]bool
	lastRecordErrorLog map[metav1.StatusReason]time.Time
}

type state struct {
	// nudges wakes this workload's writer. It holds one: a second would tell
	// the writer nothing the first does not.
	nudges chan struct{}
	// stop ends the writer when the workload leaves the cache.
	stop chan struct{}

	lock sync.Mutex

	lastRequestAt time.Time
	// lastWriteAt is the last request time this replica successfully wrote.
	lastWriteAt time.Time
	// retryAfter holds off a write that just failed. A retry a mark later is
	// what keeps a transient failure from costing a heartbeat of margin; one
	// per request is what turned a standing failure into a storm.
	retryAfter time.Time

	sandboxName string
	threshold   *time.Duration

	suspendedAt time.Time

	// A real suspend takes the workload out of the Running set and is edge
	// triggered by that. A suppressed one leaves it there, so without this every
	// later tick would count and log it again.
	suppressedAt time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
	AppStateWatcher() *k8sapp.StateWatcher
}

func NewManager(ctx context.Context, d dependencies, k8sClient dynamic.Interface) *Manager {
	m := newManager(
		ctx,
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		d.AppStateWatcher(),
		newMetrics(d.Telemetry().Meter()),
		d.Config().IdleSuspend.Enabled,
	)

	m.records = newRecordCache(ctx, k8sClient, d.Config().K8s.AppsNamespace)

	d.AppStateWatcher().OnWorkloadRemoved(m.evictWorkload)

	d.Process().OnShutdown(func(ctx context.Context) {
		m.Shutdown(ctx)
	})

	ticker := m.clock.NewTicker(tickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				m.tick(ctx)
			}
		}
	}()

	return m
}

func newManager(ctx context.Context, clock clockwork.Clock, logger log.Logger, c *client, workloads k8sWorkloads, metrics *metrics, suspendEnabled bool) *Manager {
	m := &Manager{
		stopping:           make(chan struct{}),
		suspendEnabled:     suspendEnabled,
		clock:              clock,
		logger:             logger,
		client:             c,
		workloads:          workloads,
		metrics:            metrics,
		lastRecordErrorLog: map[metav1.StatusReason]time.Time{},
	}

	// GetOrInit runs this once per workload, under the map lock, which is what
	// makes starting the workload's writer here safe.
	m.stateMap = syncmap.New[k8sapp.WorkloadRef, state](func(ref k8sapp.WorkloadRef) *state {
		item := &state{
			nudges: make(chan struct{}, 1),
			stop:   make(chan struct{}),
		}
		go m.writeLoop(ctx, ref, item)
		return item
	})

	return m
}

// Shutdown waits for the activity writes in flight. They are detached from
// their requests, so nothing else would wait for them.
func (m *Manager) Shutdown(ctx context.Context) {
	m.closeLock.Lock()
	m.closed = true
	m.closeLock.Unlock()

	m.logger.Info(ctx, "waiting for pending idle timer writes")
	m.wg.Wait()

	close(m.stopping)
}

func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	if item, found := m.stateMap.Delete(ref); found {
		close(item.stop)
	}
}
