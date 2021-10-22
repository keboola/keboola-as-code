package local

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const MaxLocalWorkers = 32

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
	workers   *errgroup.Group
	errors    *utils.Error
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

func (m *Manager) NewUnitOfWork(parentCtx context.Context) *UnitOfWork {
	workers, ctx := errgroup.WithContext(parentCtx)
	u := &UnitOfWork{
		Manager:   m,
		ctx:       ctx,
		semaphore: semaphore.NewWeighted(MaxLocalWorkers),
		workers:   workers,
		errors:    utils.NewMultiError(),
	}
	return u
}

func (u *UnitOfWork) Wait() error {
	// Wait for workers
	if err := u.workers.Wait(); err != nil {
		u.errors.Append(err)
	}
	return u.errors.ErrorOrNil()
}

func (u *UnitOfWork) SaveObject(objectState model.ObjectState) {
	u.addWorker(func() error {
		if err := u.Manager.SaveObject(objectState.Manifest(), objectState.RemoteState()); err != nil {
			return err
		}
		objectState.SetLocalState(objectState.RemoteState())
		return nil
	})
}

func (u *UnitOfWork) DeleteObject(objectState model.ObjectState) {
	u.addWorker(func() error {
		if err := u.Manager.DeleteObject(objectState.Manifest()); err != nil {
			return err
		}
		objectState.SetLocalState(nil)
		return nil
	})
}

func (u *UnitOfWork) addWorker(worker func() error) {
	u.workers.Go(func() error {
		// Limit maximum numbers of parallel filesystem operations.
		// It prevents problem with: too many open files
		if err := u.semaphore.Acquire(u.ctx, 1); err != nil {
			return err
		}
		defer u.semaphore.Release(1)

		if err := worker(); err != nil {
			u.errors.Append(err)
		}
		return nil
	})
}
