package local

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/workers"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type uow struct {
	invoked bool
	ctx     context.Context
	state   *State
	filter  model.ObjectsFilter
	mapper  *mapper.Mapper
	workers *orderedmap.OrderedMap // separated workers for changes in branches, configs and rows
	errors  *utils.MultiError
}

func newUnitOfWorkBackend(ctx context.Context, state *State, filter model.ObjectsFilter, mapper *mapper.Mapper) state.UnitOfWorkBackend {
	return &uow{
		ctx:     ctx,
		state:   state,
		filter:  filter,
		mapper:  mapper,
		workers: orderedmap.New(),
		errors:  utils.NewMultiError(),
	}
}

func (u *uow) Invoke() (state.FinalizationFn, error) {
	// Check conditions
	if u.invoked {
		panic(fmt.Errorf(`UnitOfWork: invoke can only be called once`))
	}
	u.invoked = true

	// Start and wait for all workers
	u.workers.SortKeys(sort.Strings)
	for _, level := range u.workers.Keys() {
		worker, _ := u.workers.Get(level)
		if err := worker.(*workers.Workers).StartAndWait(); err != nil {
			u.errors.Append(err)
		}
	}

	// Finalization callback with changes
	finalizeFn := func(changes *model.Changes) error {
		errors := utils.NewMultiError()

		// AfterRemoteOperation event
		if !changes.Empty() {
			if err := u.mapper.AfterLocalOperation(changes); err != nil {
				errors.Append(err)
			}
		}

		// Delete empty directories, e.g., no extractor of a type left -> dir is empty
		if err := deleteEmptyDirectories(u.state.fs, u.state.knownPaths); err != nil {
			errors.Append(err)
		}

		// Update tracked paths
		if err := u.state.reloadKnownPaths(); err != nil {
			errors.Append(err)
		}

		return errors.ErrorOrNil()
	}

	return finalizeFn, u.errors.ErrorOrNil()
}

func (u *uow) LoadAll(onLoad func(object model.Object) bool) {
	c := loadCtx{uow: u, onLoad: onLoad}
	c.loadAll()
}

func (u *uow) Save(object model.Object, changedFields model.ChangedFields, exists bool, onSuccess func()) {
	// Save
	c := saveCtx{uow: u, object: object, changedFields: changedFields, objectExists: exists, onSuccess: onSuccess}
	c.save()
}

func (u *uow) Delete(key model.Key, onSuccess func()) {
	c := deleteCtx{uow: u, key: key, onSuccess: onSuccess}
	c.delete()
}

// workersFor each level (branches, configs, rows).
func (u *uow) workersFor(level model.ObjectLevel) *workers.Workers {
	if u.invoked {
		panic(fmt.Errorf(`invoked local.UnitOfWork cannot be reused`))
	}

	key := cast.ToString(level)
	if value, found := u.workers.Get(key); found {
		return value.(*workers.Workers)
	}

	w := workers.New(u.ctx)
	u.workers.Set(key, w)
	return w
}
