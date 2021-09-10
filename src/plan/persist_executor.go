package plan

import (
	"fmt"

	"go.uber.org/zap"

	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
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
			if err := e.LocalManager().DeleteModel(a.Record); err != nil {
				e.errors.Append(err)
			}
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
		record, found := e.Manifest().CreateOrGetRecord(key)
		if found {
			panic(fmt.Errorf(`unexpected state: record "%s" existis, but it should not`, record))
		}

		// Set local path
		record.SetObjectPath(action.Path)

		// Save to manifest.json
		e.Manifest().PersistRecord(record)

		// Load model
		if _, err := e.LoadModel(record); err != nil {
			e.errors.Append(err)
			return
		}

		// Set config id to rows
		for _, rowAction := range action.Rows {
			rowAction.Key.ConfigId = key.Id
		}
	})
}

func (e *persistExecutor) persistNewRow(action *NewRowAction) {
	// Generate unique ID
	e.tickets.Request(func(ticket *model.Ticket) {
		key := action.Key
		key.Id = ticket.Id

		// The parent config was not persisted for some error -> skip row
		if key.ConfigId == "" {
			return
		}

		// Create manifest record
		record, found := e.Manifest().CreateOrGetRecord(key)
		if found {
			panic(fmt.Errorf(`unexpected state: record "%s" existis, but it should not`, record))
		}

		// Set local path
		record.SetObjectPath(action.Path)

		// Save to manifest.json
		e.Manifest().PersistRecord(record)

		// Load model
		if _, err := e.LoadModel(record); err != nil {
			e.errors.Append(err)
		}
	})
}
