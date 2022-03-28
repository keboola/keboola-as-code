package remote

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type uow struct {
	invoked           bool
	ctx               context.Context
	changeDescription string
	filter            model.ObjectsFilter
	storageApi        *storageapi.Api
	storageApiPools   *orderedmap.OrderedMap // separated pool for changes in branches, configs and rows
	mapper            *mapper.Mapper
	errors            *utils.MultiError
}

func newUnitOfWorkBackend(ctx context.Context, filter model.ObjectsFilter, changeDescription string, storageApi *storageapi.Api, mapper *mapper.Mapper) state.UnitOfWorkBackend {
	return &uow{
		ctx:               ctx,
		changeDescription: changeDescription,
		filter:            filter,
		storageApi:        storageApi,
		storageApiPools:   orderedmap.New(),
		mapper:            mapper,
		errors:            utils.NewMultiError(),
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

	// Finalization callback with changes
	finalizeFn := func(changes *model.Changes) error {
		// AfterRemoteOperation event
		if !changes.Empty() {
			return u.mapper.AfterRemoteOperation(changes)
		}
		return nil
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
