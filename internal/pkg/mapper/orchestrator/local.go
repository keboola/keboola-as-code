package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *orchestratorMapper) AfterLocalOperation(ctx context.Context, changes *model.LocalChanges) error {
	errs := errors.NewMultiError()
	allObjects := m.state.LocalObjects()

	// Map loaded objects
	for _, objectState := range changes.Loaded() {
		if ok, err := m.isOrchestratorConfigKey(objectState.Key()); err != nil {
			errs.Append(err)
			continue
		} else if ok {
			configState := objectState.(*model.ConfigState)
			if err := m.onLocalLoad(ctx, configState.Local, configState.ConfigManifest, allObjects); err != nil {
				errs.Append(err)
			}
		}
	}

	// Find renamed orchestrators and renamed configs used in an orchestrator
	if len(changes.Renamed()) > 0 {
		if err := m.onObjectsRename(ctx, changes.Renamed(), allObjects); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}
