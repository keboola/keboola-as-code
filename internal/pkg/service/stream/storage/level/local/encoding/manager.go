package encoding

import (
	"context"
	"sort"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Manager struct {
	logger       log.Logger
	clock        clock.Clock
	events       *events.Events[Pipeline]
	config       Config
	outputOpener OutputOpener

	pipelinesLock *sync.Mutex
	pipelines     map[string]*pipelineRef
}

type pipelineRef struct {
	Pipeline
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
}

func NewManager(d dependencies, config Config, outputOpener OutputOpener) (*Manager, error) {
	m := &Manager{
		logger:        d.Logger(),
		clock:         d.Clock(),
		events:        events.New[Pipeline](),
		config:        config,
		outputOpener:  outputOpener,
		pipelinesLock: &sync.Mutex{},
		pipelines:     make(map[string]*pipelineRef),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		m.logger.Info(ctx, "closing encoding pipelines")
		if err := m.close(ctx); err != nil {
			err := errors.PrefixError(err, "cannot close encoding pipelines")
			logger.Error(ctx, err.Error())
		}
		m.logger.Info(ctx, "closed encoding pipelines")
	})

	return m, nil
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

func (m *Manager) OpenPipeline(ctx context.Context, slice *model.Slice) (w Pipeline, err error) {
	// Check if the pipeline already exists, if not, register an empty reference to unlock immediately
	ref, exists := m.addPipeline(slice.SliceKey)
	if exists {
		return nil, errors.Errorf(`encoding pipeline for slice "%s" already exists`, slice.SliceKey.String())
	}

	// Close resources on a creation error
	defer func() {
		// Ok, update reference
		if err == nil {
			ref.Pipeline = w
			return
		}

		// Remove the pipeline ref
		m.removePipeline(slice.SliceKey)
	}()

	// Open output
	out, err := m.outputOpener.OpenOutput(slice.SliceKey)
	if err != nil {
		return nil, err
	}

	// Create pipeline
	w, err = NewPipeline(ctx, m.logger, m.clock, m.config, slice, out, m.events)
	if err != nil {
		return nil, err
	}

	// Register PIPELINE close callback
	w.Events().OnClose(func(w Pipeline, _ error) error {
		m.removePipeline(w.SliceKey())
		return nil
	})

	return w, nil
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
