package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *mapper) OnObjectsRename(event model.OnObjectsRenameEvent) error {
	errors := utils.NewMultiError()

	// Find renamed shared codes
	renamedSharedCodes := make(map[string]model.Key)
	for _, object := range event.RenamedObjects {
		key := object.Record.Key()

		// Is shared code?
		if ok, err := m.isSharedCodeKey(key); err != nil {
			errors.Append(err)
		} else if ok {
			renamedSharedCodes[key.String()] = key
			continue
		}

		// Is shared code row?
		if ok, err := m.isSharedCodeRowKey(key); err != nil {
			errors.Append(err)
		} else if ok {
			configKey := key.(model.ConfigRowKey).ConfigKey()
			renamedSharedCodes[configKey.String()] = configKey
		}
	}

	// Log
	if len(renamedSharedCodes) > 0 {
		m.Logger.Debug(`Found renamed shared codes:`)
		for _, key := range renamedSharedCodes {
			m.Logger.Debugf(`  - %s`, key.Desc())
		}
	}

	// Find transformations using these shared codes
	uow := m.localManager.NewUnitOfWork(context.Background())
	for _, objectState := range m.State.All() {
		configState, err := m.getDependentConfig(objectState, renamedSharedCodes)
		if err != nil {
			errors.Append(err)
		} else if configState == nil {
			continue
		}

		// Re-save config -> new "shared_code_path" will be saved.
		m.Logger.Debugf(`Need to update shared codes in "%s"`, configState.Path())
		uow.SaveObject(configState, configState.Local, model.NewChangedFields("configuration"))
	}

	// Save
	if err := uow.Invoke(); err != nil {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}

func (m *mapper) getDependentConfig(objectState model.ObjectState, renamedSharedCodes map[string]model.Key) (*model.ConfigState, error) {
	// Must be transformation + have "shared_code_id" key
	_, sharedCodeKey, err := m.getSharedCodeKey(objectState.LocalState())
	if err != nil || sharedCodeKey == nil {
		return nil, err
	}

	// Check if shared code has been renamed.
	if _, found := renamedSharedCodes[sharedCodeKey.String()]; found {
		return objectState.(*model.ConfigState), nil
	}
	return nil, nil
}
