package manager

import (
	"context"
	"github.com/benbjohnson/clock"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/oauth2-proxy/oauth2-proxy/v7/pkg/logger"
	"sort"
	"sync"
)

type Manager struct {
	logger       log.Logger
	clock        clock.Clock
	writerEvents *events.Events[encoding.Writer]
	config       encoding.Config

	writersLock *sync.Mutex
	writers     map[string]*writerRef
}

type dependencies interface {
	Logger() log.Logger
	Clock() clock.Clock
	Process() *servicectx.Process
}

func New(d dependencies, config encoding.Config) (*Manager, error) {
	m := &Manager{
		logger:       d.Logger(),
		clock:        d.Clock(),
		writerEvents: events.New[encoding.Writer](),
		config:       config,
		writersLock:  &sync.Mutex{},
		writers:      make(map[string]*writerRef),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		m.logger.Info(ctx, "closing encoding writers")
		if err := m.close(ctx); err != nil {
			err := errors.PrefixError(err, "cannot close encoding writers")
			logger.Error(ctx, err.Error())
		}
		m.logger.Info(ctx, "closed encoding writers")
	})

	return m, nil
}

func (m *Manager) Writers() (out []encoding.Writer) {
	m.writersLock.Lock()
	defer m.writersLock.Unlock()

	out = make([]encoding.Writer, 0, len(m.writers))
	for _, w := range m.writers {
		out = append(out, w)
	}

	sort.SliceStable(out, func(i, j int) bool {
		return out[i].SliceKey().String() < out[j].SliceKey().String()
	})

	return out
}

// Close all writers.
func (m *Manager) close(ctx context.Context) error {
	errs := errors.NewMultiError()
	wg := &sync.WaitGroup{}
	for _, w := range m.writers {
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
