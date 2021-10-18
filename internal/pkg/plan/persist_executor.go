package plan

import (
	"fmt"

	"go.uber.org/zap"

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
	errors  *utils.Error
}

func newPersistExecutor(logger *zap.SugaredLogger, api *remote.StorageApi, projectState *state.State, plan *PersistPlan) *persistExecutor {
	return &persistExecutor{
		PersistPlan: plan,
		State:       projectState,
		logger:      logger,
		tickets:     api.NewTicketProvider(),
		errors:      utils.NewMultiError(),
	}
}

func (e *persistExecutor) invoke() error {
	for _, action := range e.actions {
		switch a := action.(type) {
		case *NewConfigAction:
			e.persistNewConfig(a)
		case *NewRowAction:
			e.persistNewRow(a)
		case *DeleteRecordAction:
			if err := e.LocalManager().DeleteObject(a.Record); err != nil {
				e.errors.Append(err)
			}
		case *NewVariablesRelAction:
			e.persistNewRow(a)
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, action))
		}
	}

	// Let's wait until all new IDs are generated
	if err := e.tickets.Resolve(); err != nil {
		e.errors.Append(err)
	}

	return e.errors.ErrorOrNil()
}

func (e *persistExecutor) persistNewConfig(action *NewConfigAction) {
	// Generate unique ID
	e.tickets.Request(func(ticket *model.Ticket) {
		key := action.Key
		key.Id = ticket.Id

		// Create manifest record
		record, found, err := e.Manifest().CreateOrGetRecord(key)
		if err != nil {
			e.errors.Append(err)
			return
		} else if found {
			panic(fmt.Errorf(`unexpected state: record "%s" existis, but it should not`, record))
		}

		// Set local path
		record.SetObjectPath(action.ObjectPath)

		// Save to manifest.json
		if err := e.Manifest().PersistRecord(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Load model
		if _, err := e.LoadModel(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Setup related objects
		action.InvokeOnPersist(key)
	})
}

func (e *persistExecutor) persistNewRow(action *NewRowAction) {
	// Generate unique ID
	e.tickets.Request(func(ticket *model.Ticket) {
		key := action.Key

		// Set new id to the key
		key.Id = ticket.Id

		// The parent config was not persisted for some error -> skip row
		if key.ConfigId == "" {
			return
		}

		// Create manifest record
		record, found, err := e.Manifest().CreateOrGetRecord(key)
		if err != nil {
			e.errors.Append(err)
			return
		} else if found {
			panic(fmt.Errorf(`unexpected state: record "%s" existis, but it should not`, record))
		}

		// Set local path
		record.SetObjectPath(action.ObjectPath)

		// Save to manifest.json
		if err := e.Manifest().PersistRecord(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Load model
		if _, err := e.LoadModel(record); err != nil {
			e.errors.Append(err)
		}
	})
}

func (e *persistExecutor) addVariablesRelation(action *NewRowAction) {

}
