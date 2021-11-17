package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) OnObjectsRename(event model.OnObjectsRenameEvent) error {
	errors := utils.NewMultiError()

	// Find renamed orchestrators
	renamedOrchestrator := make(map[string]model.Key)
	for _, object := range event.RenamedObjects {
		key := object.Record.Key()

		// Is orchestrator?
		if ok, err := m.isOrchestratorConfigKey(key); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			renamedOrchestrator[key.String()] = key
			continue
		}
	}

	// Log and save
	uow := m.localManager.NewUnitOfWork(context.Background())
	if len(renamedOrchestrator) > 0 {
		m.Logger.Debug(`Found renamed orchestrators:`)
		for _, key := range renamedOrchestrator {
			m.Logger.Debugf(`  - %s`, key.Desc())
			orchestrator := m.State.MustGet(key)
			uow.SaveObject(orchestrator, orchestrator.LocalState(), model.NewChangedFields(`orchestrator`))
		}
	}

	// Invoke
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
