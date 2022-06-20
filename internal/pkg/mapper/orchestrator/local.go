package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) AfterLocalOperation(_ context.Context, changes *model.LocalChanges) error {
	errors := utils.NewMultiError()
	allObjects := m.state.LocalObjects()

	// Map loaded objects
	for _, objectState := range changes.Loaded() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			if err := m.onLocalLoad(configState.Local, configState.ConfigManifest, allObjects); err != nil {
				errors.Append(err)
			}
		}
	}

	// Find renamed orchestrators and renamed configs used in an orchestrator
	if len(changes.Renamed()) > 0 {
		if err := m.onObjectsRename(changes.Renamed(), allObjects); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}
