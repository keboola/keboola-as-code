package plan

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type persistExecutor struct {
	*PersistPlan
	*state.State
	logger  *zap.SugaredLogger
	tickets *remote.TicketProvider
	uow     *local.UnitOfWork
	errors  *utils.Error
}

func newPersistExecutor(logger *zap.SugaredLogger, api *remote.StorageApi, projectState *state.State, plan *PersistPlan) *persistExecutor {
	return &persistExecutor{
		PersistPlan: plan,
		State:       projectState,
		logger:      logger,
		tickets:     api.NewTicketProvider(),
		uow:         projectState.LocalManager().NewUnitOfWork(context.Background()),
		errors:      utils.NewMultiError(),
	}
}

func (e *persistExecutor) invoke() error {
	for _, action := range e.actions {
		switch a := action.(type) {
		case *NewObjectAction:
			e.persistNewObject(a)
		case *DeleteRecordAction:
			objectState, _ := e.State.Get(a.Key())
			e.uow.DeleteObject(objectState, a.Record)
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, action))
		}
	}

	// Let's wait until all new IDs are generated
	if err := e.tickets.Resolve(); err != nil {
		e.errors.Append(err)
	}

	// Wait for all local operations
	if err := e.uow.Invoke(); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *persistExecutor) persistNewObject(action *NewObjectAction) {
	// Generate unique ID
	e.tickets.Request(func(ticket *model.Ticket) {
		key := action.Key

		// Set new id to the key
		switch k := key.(type) {
		case model.ConfigKey:
			k.Id = ticket.Id
			key = k
		case model.ConfigRowKey:
			k.Id = ticket.Id
			key = k
		default:
			panic(fmt.Errorf(`unexpected type "%s" of the persisted object "%s"`, key.Kind(), key.Desc()))
		}

		// The parent was not persisted for some error -> skip
		if action.ParentKey != nil && action.ParentKey.ObjectId() == `` {
			return
		}

		// Create manifest record
		record, found, err := e.Manifest().CreateOrGetRecord(key)
		if err != nil {
			e.errors.Append(err)
			return
		} else if found {
			panic(fmt.Errorf(`unexpected state: manifest record "%s" exists, but it should not`, record))
		}

		// Invoke mapper
		err = e.Mapper().MapBeforePersist(&model.PersistRecipe{
			ParentKey: action.ParentKey,
			Manifest:  record,
		})
		if err != nil {
			e.errors.Append(err)
			return
		}

		// Update parent path - may be affected by relations
		if err := e.Manifest().ResolveParentPath(record); err != nil {
			e.errors.Append(fmt.Errorf(`cannot resolve path: %w`, err))
			return
		}

		// Set local path
		record.SetObjectPath(action.ObjectPath)

		// Save to manifest.json
		if err := e.Manifest().PersistRecord(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Load model
		e.uow.LoadObject(record)

		// Setup related objects
		action.InvokeOnPersist(key)
	})
}
