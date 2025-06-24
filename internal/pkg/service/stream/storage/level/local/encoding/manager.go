package encoding

import (
	"context"
	"sort"
	"sync"

	"github.com/jonboulle/clockwork"
	"github.com/sasha-s/go-deadlock"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Manager opens and closes encoding pipelines.
type Manager struct {
	logger log.Logger
	clock  clockwork.Clock
	events *events.Events[Pipeline]

	pipelinesLock *deadlock.Mutex
	pipelines     map[string]*pipelineRef
}

type pipelineRef struct {
	Pipeline
}

type dependencies interface {
	Logger() log.Logger
	Clock() clockwork.Clock
	Process() *servicectx.Process
}

func NewManager(d dependencies) *Manager {
	m := &Manager{
		logger:        d.Logger(),
		clock:         d.Clock(),
		events:        events.New[Pipeline](),
		pipelinesLock: &deadlock.Mutex{},
		pipelines:     make(map[string]*pipelineRef),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		m.logger.Info(ctx, "closing encoding pipelines")
		if err := m.close(ctx); err != nil {
			err := errors.PrefixError(err, "cannot close encoding pipelines")
			m.logger.Error(ctx, err.Error())
		}
		m.logger.Info(ctx, "closed encoding pipelines")
	})

	return m
}

func (m *Manager) Events() *events.Events[Pipeline] {
	return m.events
}

func (m *Manager) Pipelines() (out []Pipeline) {
	m.pipelinesLock.Lock()
	defer m.pipelinesLock.Unlock()

	out = make([]Pipeline, 0, len(m.pipelines))
	for _, w := range m.pipelines {
		if w.Pipeline != nil { // nil == creating a new pipeline
			out = append(out, w)
		}
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
}

func (m *Manager) OpenPipeline(
	ctx context.Context,
	sliceKey model.SliceKey,
	telemetry telemetry.Telemetry,
	connections *connection.Manager,
	mappingCfg table.Mapping,
	encodingCfg encoding.Config,
	localStorage localModel.Slice,
	withBackup bool,
	closeFunc func(ctx context.Context, cause string),
	network rpc.NetworkOutput,
) (w Pipeline, err error) {
	// Check if the pipeline already exists, if not, register an empty reference to unlock immediately
	ref, exists := m.addPipeline(sliceKey)
	if exists {
		return nil, errors.Errorf(`encoding pipeline for slice "%s" already exists`, sliceKey.String())
	}

	// Create pipeline
	ref.Pipeline, err = newPipeline(
		ctx,
		m.logger,
		m.clock,
		sliceKey,
		telemetry,
		connections,
		mappingCfg,
		encodingCfg,
		localStorage,
		m.events,
		withBackup,
		closeFunc,
		network,
	)
	if err != nil {
		m.removePipeline(sliceKey)
		return nil, err
	}

	// Register pipeline close callback
	ref.Pipeline.Events().OnClose(func(w Pipeline, _ error) error {
		m.removePipeline(w.SliceKey())
		return nil
	})

	return ref.Pipeline, nil
}

func (m *Manager) addPipeline(k model.SliceKey) (ref *pipelineRef, exists bool) {
	m.pipelinesLock.Lock()
	defer m.pipelinesLock.Unlock()

	key := k.String()
	ref, exists = m.pipelines[key]
	if !exists {
		// Register a new empty reference, it will be initialized later.
		// Empty reference is used to make possible to create multiple pipelines without being blocked by the lock.
		ref = &pipelineRef{}
		m.pipelines[key] = ref
	}

	return ref, exists
}

func (m *Manager) removePipeline(k model.SliceKey) {
	m.pipelinesLock.Lock()
	defer m.pipelinesLock.Unlock()
	delete(m.pipelines, k.String())
}

// Close all pipelines.
func (m *Manager) close(ctx context.Context) error {
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	for _, w := range m.Pipelines() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Close(ctx); err != nil {
				errs.Append(err)
			}
		}()
	}
	wg.Wait()
	return errs.ErrorOrNil()
}
