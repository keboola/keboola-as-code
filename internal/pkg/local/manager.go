package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/iancoleman/orderedmap"
	"github.com/spf13/cast"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Manager struct {
	logger   *zap.SugaredLogger
	fs       filesystem.Fs
	manifest *manifest.Manifest
	state    *model.State
	mapper   *mapper.Mapper
}

type UnitOfWork struct {
	*Manager
	semaphore *semaphore.Weighted
	ctx       context.Context
	workers   *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
	errors    *utils.Error
	invoked   bool
}

func NewManager(logger *zap.SugaredLogger, fs filesystem.Fs, m *manifest.Manifest, state *model.State, mapper *mapper.Mapper) *Manager {
	return &Manager{
		logger:   logger,
		fs:       fs,
		manifest: m,
		state:    state,
		mapper:   mapper,
	}
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.manifest
}

func (m *Manager) Naming() *model.Naming {
	return m.manifest.Naming
}

func (m *Manager) NewUnitOfWork(ctx context.Context) *UnitOfWork {
	u := &UnitOfWork{
		Manager:   m,
		ctx:       ctx,
		semaphore: semaphore.NewWeighted(MaxLocalWorkers),
		workers:   utils.NewOrderedMap(),
		errors:    utils.NewMultiError(),
	}
	return u
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState) {
	u.
		workersFor(objectState.Level()).
		AddWorker(func() error {
			if err := u.Manager.SaveObject(objectState.Manifest(), objectState.RemoteState()); err != nil {
				return err
			}
			objectState.SetLocalState(objectState.RemoteState())
			return nil
		})
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState) {
	u.
		workersFor(objectState.Level()).
		AddWorker(func() error {
			if err := u.Manager.DeleteObject(objectState.Manifest()); err != nil {
				return err
			}
			objectState.SetLocalState(nil)
			return nil
		})
}

func (u *UnitOfWork) Invoke() error {
	if u.invoked {
		panic(fmt.Errorf(`invoked local.UnitOfWork cannot be reused`))
	}

	u.workers.SortKeys(sort.Strings)
	for _, level := range u.workers.Keys() {
		worker, _ := u.workers.Get(level)
		if err := worker.(*Workers).StartAndWait(); err != nil {
			u.errors.Append(err)
		}
	}

	u.invoked = true
	return u.errors.ErrorOrNil()
}

// workersFor each level (branches, configs, rows).
func (u *UnitOfWork) workersFor(level int) *Workers {
	if u.invoked {
		panic(fmt.Errorf(`invoked local.UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.workers.Get(key); found {
		return value.(*Workers)
	}

	workers := NewWorkers(u.ctx)
	u.workers.Set(key, workers)
	return workers
}
