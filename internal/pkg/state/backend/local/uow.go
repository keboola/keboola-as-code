package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/workers"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	saveManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/common/local/manifest/save"
)

type _state = State

type uow struct {
	*_state
	invoked bool
	ctx     context.Context
	filter  model.ObjectsFilter
	mapper  *mapper.Mapper
	workers *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
}

func newUnitOfWorkBackend(state *State, ctx context.Context, filter model.ObjectsFilter, mapper *mapper.Mapper) state.UnitOfWorkBackend {
	return &uow{
		_state:  state,
		ctx:     ctx,
		filter:  filter,
		mapper:  mapper,
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
	errors := utils.NewMultiError()
	u.workers.SortKeys(sort.Strings)
	for _, level := range u.workers.Keys() {
		worker, _ := u.workers.Get(level)
		if err := worker.(*workers.Workers).StartAndWait(); err != nil {
			errors.Append(err)
		}
	}

	return u.finalizeFn(), errors.ErrorOrNil()
}

func (u *uow) LoadAll(loadCtx state.LoadContext) {
	(&loadContext{uow: u, LoadContext: loadCtx}).loadAll()
}

func (u *uow) Save(saveCtx state.SaveContext) {
	(&saveContext{uow: u, SaveContext: saveCtx}).save()
}

func (u *uow) Delete(deleteCtx state.DeleteContext) {
	(&deleteContext{uow: u, DeleteContext: deleteCtx}).delete()
}

// finalizeFn callback - responds to the changes that have been made.
func (u *uow) finalizeFn() state.FinalizationFn {
	return func(changes *model.Changes) error {
		errors := utils.NewMultiError()

		// AfterLocalOperation event
		if !changes.Empty() {
			if err := u.mapper.AfterLocalOperation(changes); err != nil {
				spew.Dump(err)
				errors.Append(err)
			}
		}

		// Delete empty directories, e.g., no extractor of a type left -> dir is empty
		if err := deleteEmptyDirectories(u.objectsRoot, u.knownPaths.TrackedPaths()); err != nil {
			errors.Append(err)
		}

		// Update tracked paths
		if err := u.reloadKnownPaths(); err != nil {
			errors.Append(err)
		}

		// Save manifest if has been changed
		if _, err := saveManifest.Run(u.manifest, u.deps); err != nil {
			errors.Append(err)
		}

		return errors.ErrorOrNil()
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
