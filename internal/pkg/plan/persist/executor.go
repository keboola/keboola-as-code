package persist

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type executor struct {
	*Plan
	*state.State
	logger  log.Logger
	tickets *storageapi.TicketProvider
	uow     *local.UnitOfWork
	errors  *utils.MultiError
}

func newExecutor(logger log.Logger, api *storageapi.Api, projectState *state.State, plan *Plan) *executor {
	return &executor{
		Plan:    plan,
		State:   projectState,
		logger:  logger,
		tickets: api.NewTicketProvider(),
		uow:     projectState.LocalManager().NewUnitOfWork(context.Background()),
		errors:  utils.NewMultiError(),
	}
}

func (e *executor) invoke() error {
	for _, action := range e.actions {
		switch a := action.(type) {
		case *newObjectAction:
			e.persistNewObject(a)
		case *deleteManifestRecordAction:
			objectState, _ := e.State.Get(a.Key())
			e.uow.DeleteObject(objectState, a.ObjectManifest)
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

func (e *executor) persistNewObject(action *newObjectAction) {
	// Generate unique ID
	e.tickets.Request(func(ticket *storageapi.Ticket) {
		key := action.Key

		// Set new id to the key
		switch k := key.(type) {
		case model.ConfigKey:
			k.Id = storageapi.ConfigID(ticket.ID)
			key = k
		case model.ConfigRowKey:
			k.Id = storageapi.RowID(ticket.ID)
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
		err = e.Mapper().MapBeforePersist(e.Ctx(), &model.PersistRecipe{
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
		record.SetRelativePath(action.GetRelativePath())

		// Load model
		e.uow.LoadObject(record, model.NoFilter())

		// Save to manifest.json
		if err := e.Manifest().PersistRecord(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Setup related objects
		action.InvokeOnPersist(key)
	})
}
