package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) OnObjectsRename(event model.OnObjectsRenameEvent) error {
	errors := utils.NewMultiError()

	// Find renamed shared codes
	renamedSharedCodes := make(map[string]bool)
	for _, object := range event.RenamedObjects {
		// Is config?
		if record, ok := object.Record.(*model.ConfigManifest); ok {
			// Get component
			component, err := m.State.Components().Get(record.ComponentKey())
			if err != nil {
				errors.Append(err)
				continue
			}

			// Is shared code?
			if component.IsSharedCode() {
				renamedSharedCodes[record.Key().String()] = true
			}
		}
	}

	// Find configs using these shared codes
	uow := m.NewUnitOfWork(context.Background())
	for _, objectState := range m.State.All() {
		configState, err := m.getDependentConfig(objectState, renamedSharedCodes)
		if err != nil {
			errors.Append(err)
		} else if configState == nil {
			continue
		}

		// Re-save config -> new "shared_code_path" will be saved.
		m.Logger.Debugf(`Updating "shared_code_path" in "%s"`, configState.Path())
		uow.SaveObject(configState, configState.Local, model.NewChangedFields("configuration"))
	}

	// Save
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

func (m *mapper) getDependentConfig(object model.ObjectState, renamedSharedCodes map[string]bool) (*model.ConfigState, error) {
	// Shared code is used by config
	configState, ok := object.(*model.ConfigState)
	if !ok {
		return nil, nil
	}

	// Component must be transformation
	component, err := m.State.Components().Get(configState.ComponentKey())
	if err != nil {
		return nil, err
	}
	if !component.IsTransformation() {
		return nil, nil
	}

	// Must have "shared_code_id" key
	sharedCodeIdRaw, found := configState.Local.Content.Get(model.SharedCodeIdContentKey)
	if !found {
		return nil, nil
	}
	sharedCodeId, ok := sharedCodeIdRaw.(string)
	if !ok {
		return nil, nil
	}
	sharedCodeKey := model.ConfigKey{
		BranchId:    configState.BranchId,
		ComponentId: model.SharedCodeComponentId,
		Id:          sharedCodeId,
	}

	// Check if shared code has been renamed.
	if _, found := renamedSharedCodes[sharedCodeKey.String()]; found {
		return configState, nil
	}
	return nil, nil
}
