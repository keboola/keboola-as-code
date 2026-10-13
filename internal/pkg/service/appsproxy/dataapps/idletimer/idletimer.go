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

// activityRecorder is the part of the Recorder this loop needs: it tells the
// Recorder what each workload writes to, and reads back what this replica has
// seen. Nothing here reaches into how the Recorder keeps that.
type activityRecorder interface {
	Track(ref k8sapp.WorkloadRef, sandboxName string, threshold *time.Duration) time.Time
}

// Manager decides which running workloads have gone idle and suspends them.
type Manager struct {
	suspendEnabled bool
	clock          clockwork.Clock
	logger         log.Logger
	client         *client
	workloads      k8sWorkloads
	records        *recordCache
	recorder       activityRecorder
	metrics        *metrics
	states         *syncmap.SyncMap[k8sapp.WorkloadRef, suspendState]

	// These are touched only by the round.
	warnedUnresolved   map[api.AppID]bool
	lastRecordErrorLog map[metav1.StatusReason]time.Time
}

// suspendState is what this loop remembers about a workload between rounds.
type suspendState struct {
	lock sync.Mutex

	suspendedAt time.Time

	// A real suspend takes the workload out of the Running set and is edge
	// triggered by that. A suppressed one leaves it there, so without this every
	// later round would count and log it again.
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

func NewManager(ctx context.Context, d dependencies, k8sClient dynamic.Interface, recorder *Recorder) *Manager {
	m := newManager(
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		d.AppStateWatcher(),
		recorder,
		newMetrics(d.Telemetry().Meter()),
		d.Config().IdleSuspend.Enabled,
	)

	m.records = newRecordCache(ctx, k8sClient, d.Config().K8s.AppsNamespace)

	d.AppStateWatcher().OnWorkloadRemoved(m.evictWorkload)

	ticker := m.clock.NewTicker(tickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				m.suspendIdleWorkloads(ctx)
			}
		}
	}()

	return m
}

func newManager(clock clockwork.Clock, logger log.Logger, c *client, workloads k8sWorkloads, recorder activityRecorder, metrics *metrics, suspendEnabled bool) *Manager {
	return &Manager{
		suspendEnabled:     suspendEnabled,
		clock:              clock,
		logger:             logger,
		client:             c,
		workloads:          workloads,
		recorder:           recorder,
		metrics:            metrics,
		lastRecordErrorLog: map[metav1.StatusReason]time.Time{},
		states: syncmap.New[k8sapp.WorkloadRef, suspendState](func(k8sapp.WorkloadRef) *suspendState {
			return &suspendState{}
		}),
	}
}

func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	m.states.Delete(ref)
}
