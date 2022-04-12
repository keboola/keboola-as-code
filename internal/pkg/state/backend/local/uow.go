package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/workers"
	"github.com/keboola/keboola-as-code/internal/pkg/state/filter"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type _state = State

type uow struct {
	*_state
	invoked bool
	ctx     context.Context
	workers *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
}

func newUnitOfWorkBackend(state *State, ctx context.Context) state.UnitOfWorkBackend {
	return &uow{
		_state:  state,
		ctx:     ctx,
		workers: orderedmap.New(),
	}
}

func (u *uow) Invoke() (state.FinalizationFn, error) {
	// Check conditions
	if u.invoked {
		panic(fmt.Errorf(`UnitOfWork: invoke can only be called once`))
	}
	u.invoked = true

	// Start and wait for all workers
	errs := errors.NewMultiError()
	u.workers.SortKeys(sort.Strings)
	for _, level := range u.workers.Keys() {
		worker, _ := u.workers.Get(level)
		if err := worker.(*workers.Workers).StartAndWait(); err != nil {
			errs.Append(err)
		}
	}

	return u.finalizeFn(), errs.ErrorOrNil()
}

func (u *uow) LoadAll(ctx state.LoadContext) {
	// Add filter from the manifest
	ctx.Filter = filter.NewComposedFilter(ctx.Filter, u.manifest.Filter())
	(&loadContext{uow: u, parentCtx: ctx}).loadAll()
}

func (u *uow) Save(ctx state.SaveContext) {
	(&saveContext{uow: u, parentCtx: ctx}).save()
}

func (u *uow) Delete(ctx state.DeleteContext) {
	(&deleteContext{uow: u, parentCtx: ctx}).delete()
}

// finalizeFn callback - responds to the changes that have been made.
func (u *uow) finalizeFn() state.FinalizationFn {
	return func(changes *model.Changes) error {
		errs := errors.NewMultiError()

		// AfterLocalOperation event
		if !changes.Empty() {
			if err := u.mapper.AfterLocalOperation(changes); err != nil {
				errs.Append(err)
			}
		}

		// Delete empty directories, e.g., no extractor of a type left -> dir is empty
		if err := deleteEmptyDirectories(u.objectsRoot, u.knownPaths.TrackedPaths()); err != nil {
			errs.Append(err)
		}

		// Update tracked paths
		if err := u.reloadKnownPaths(); err != nil {
			errs.Append(err)
		}

		// Save manifest if has been changed
		if err := u.manifest.Save(); err != nil {
			errs.Append(err)
		}

		return errs.ErrorOrNil()
	}
}

// workersFor each level (branches, configs, rows).
func (u *uow) workersFor(level model.ObjectLevel) *workers.Workers {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be modified`))
	}

	key := cast.ToString(level)
	if value, found := u.workers.Get(key); found {
		return value.(*workers.Workers)
	}

	w := workers.New(u.ctx)
	u.workers.Set(key, w)
	return w
}
