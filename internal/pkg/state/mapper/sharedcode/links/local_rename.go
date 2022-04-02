package links

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *localMapper) AfterLocalRename(changes []model.RenameAction) error {
	errs := errors.NewMultiError()

	// Find renamed shared codes
	renamedSharedCodes := make(map[string]model.Key)
	for _, item := range changes {
		key := item.Key

		// Is shared code?
		if ok, err := m.helper.IsSharedCodeKey(key); err != nil {
			errs.Append(err)
		} else if ok {
			renamedSharedCodes[key.String()] = key
			continue
		}

		// Is shared code row?
		if ok, err := m.helper.IsSharedCodeRowKey(key); err != nil {
			errs.Append(err)
		} else if ok {
			configKey := key.(model.ConfigRowKey).ConfigKey()
			renamedSharedCodes[configKey.String()] = configKey
		}
	}

	// Log
	if len(renamedSharedCodes) > 0 {
		m.logger.Debug(`Found renamed shared codes:`)
		for _, key := range renamedSharedCodes {
			m.logger.Debugf(`  - %s`, key.String())
		}
	}

	// Find transformations using these shared codes
	uow := m.state.NewUnitOfWork(context.Background(), model.NoFilter())
	for _, object := range m.state.All() {
		config := m.getDependentConfig(object, renamedSharedCodes)
		if config == nil {
			continue
		}

		// Re-save config -> new "shared_code_path" will be saved.
		m.logger.Debugf(`Need to update shared codes in "%s"`, config)
		uow.Save(config, model.NewChangedFields("configuration"))
	}

	// Save
	if err := uow.Invoke(); err != nil {
		errs.Append(err)
	}

	return errs.ErrorOrNil()
}

func (m *localMapper) getDependentConfig(object model.Object, renamedSharedCodes map[string]model.Key) *model.Config {
	// Must be transformation + have "shared_code_id" key
	config, ok := object.(*model.Config)
	if !ok {
		return nil
	}
	if config.Transformation == nil || config.Transformation.LinkToSharedCode == nil {
		return nil
	}

	// Check if shared code has been renamed.
	if _, found := renamedSharedCodes[config.Transformation.LinkToSharedCode.Config.String()]; found {
		return config
	}
	return nil
}
