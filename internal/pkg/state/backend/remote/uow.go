package remote

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type _state = State

type uow struct {
	*_state
	invoked           bool
	ctx               context.Context
	changeDescription string
	filter            state.Filter
	storageApiPools   *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	errors            *errors.MultiError
}

func newUnitOfWorkBackend(state *State, ctx context.Context, filter state.Filter, changeDescription string) state.UnitOfWorkBackend {
	return &uow{
		_state:            state,
		ctx:               ctx,
		changeDescription: changeDescription,
		filter:            filter,
		storageApiPools:   orderedmap.New(),
		errors:            errors.NewMultiError(),
	}
}

func (u *uow) Invoke() (state.FinalizationFn, error) {
	// Check conditions
	if u.invoked {
		panic(fmt.Errorf(`UnitOfWork: invoke can only be called once`))
	}
	u.invoked = true

	// Start and wait for all pools
	u.storageApiPools.SortKeys(sort.Strings)
	for _, level := range u.storageApiPools.Keys() {
		pool, _ := u.storageApiPools.Get(level)
		if err := pool.(*client.Pool).StartAndWait(); err != nil {
			u.errors.Append(err)
			break
		}
	}

	return u.finalizeFn(), u.errors.ErrorOrNil()
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
		// AfterRemoteOperation event
		if !changes.Empty() {
			return u.mapper.AfterRemoteOperation(changes)
		}
		return nil
	}
}

// poolFor each level (branches, configs, rows).
func (u *uow) poolFor(level model.ObjectLevel) *client.Pool {
	if u.invoked {
		panic(fmt.Errorf(`invoked UnitOfWork cannot be modified`))
	}

	key := cast.ToString(level)
	if value, found := u.storageApiPools.Get(key); found {
		return value.(*client.Pool)
	}

	pool := u.storageApi.NewPool()
	pool.SetContext(u.ctx)
	u.storageApiPools.Set(key, pool)
	return pool
}
