package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) onRename(renamedObjects []model.RenameAction) error {
	errors := utils.NewMultiError()

	// Find renamed shared codes
	renamedSharedCodes := make(map[string]model.Key)
	for _, object := range renamedObjects {
		key := object.Manifest.Key()

		// Is shared code?
		if ok, err := m.helper.IsSharedCodeKey(key); err != nil {
			errors.Append(err)
		} else if ok {
			renamedSharedCodes[key.String()] = key
			continue
		}

		// Is shared code row?
		if ok, err := m.helper.IsSharedCodeRowKey(key); err != nil {
			errors.Append(err)
		} else if ok {
			configKey := key.(model.ConfigRowKey).ConfigKey()
			renamedSharedCodes[configKey.String()] = configKey
		}
	}

	// Log
	if len(renamedSharedCodes) > 0 {
		m.logger.Debug(`Found renamed shared codes:`)
		for _, key := range renamedSharedCodes {
			m.logger.Debugf(`  - %s`, key.Desc())
		}
	}

	// Find transformations using these shared codes
	uow := m.state.LocalManager().NewUnitOfWork(context.Background())
	for _, objectState := range m.state.All() {
		configState := m.getDependentConfig(objectState, renamedSharedCodes)
		if configState == nil {
			continue
		}

		// Re-save config -> new "shared_code_path" will be saved.
		m.logger.Debugf(`Need to update shared codes in "%s"`, configState.Path())
		uow.SaveObject(configState, configState.Local, model.NewChangedFields("configuration"))
	}

	// Save
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

func (m *mapper) getDependentConfig(objectState model.ObjectState, renamedSharedCodes map[string]model.Key) *model.ConfigState {
	// Must be transformation + have "shared_code_id" key
	configState, ok := objectState.(*model.ConfigState)
	if !ok || !configState.HasLocalState() {
		return nil
	}
	config := configState.Local
	if config.Transformation == nil || config.Transformation.LinkToSharedCode == nil {
		return nil
	}

	// Check if shared code has been renamed.
	if _, found := renamedSharedCodes[config.Transformation.LinkToSharedCode.Config.String()]; found {
		return configState
	}
	return nil
}
