package codes

import (
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// OnRemoteChange converts legacy "code_content" string -> []interface{}.
func (m *mapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	errors := utils.NewMultiError()
	for _, objectState := range changes.Loaded() {
		if ok, err := m.IsSharedCodeKey(objectState.Key()); err != nil {
			errors.Append(err)
			continue
		} else if ok {
			if err := m.onConfigRemoteLoad(objectState.(*model.ConfigState).Remote); err != nil {
				errors.Append(err)
			}
		}
	}

	if errors.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(utils.PrefixError(`Warning`, errors))
	}

	return nil
}

func (m *mapper) onConfigRemoteLoad(config *model.Config) error {
	// Get "code_content" value
	targetRaw, found := config.Content.Get(model.ShareCodeTargetComponentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		config.Content.Delete(model.ShareCodeTargetComponentKey)
	}()

	// Value should be string
	target, ok := targetRaw.(string)
	if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`invalid %s`, config.Desc()),
			fmt.Errorf(`key "%s" should be string, found "%T"`, model.ShareCodeTargetComponentKey, targetRaw),
		)
	}

	// Store target component ID to struct
	config.SharedCode = &model.SharedCodeConfig{Target: storageapi.ComponentID(target)}

	errors := utils.NewMultiError()
	for _, row := range m.state.RemoteObjects().ConfigRowsFrom(config.ConfigKey) {
		if err := m.onRowRemoteLoad(config, row); err != nil {
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}

func (m *mapper) onRowRemoteLoad(config *model.Config, row *model.ConfigRow) error {
	// Get "code_content" value
	raw, found := row.Content.Get(model.SharedCodeContentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		row.Content.Delete(model.SharedCodeContentKey)
	}()

	// Parse value
	var scripts model.Scripts
	switch v := raw.(type) {
	case string:
		scripts = model.ScriptsFromStr(v, config.SharedCode.Target)
	case []interface{}:
		scripts = model.ScriptsFromSlice(v)
	default:
		return utils.PrefixError(
			fmt.Sprintf(`invalid %s`, row.Desc()),
			fmt.Errorf(`key "%s" should be string or array, found "%T"`, model.SharedCodeContentKey, raw),
		)
	}

	// Store scripts to struct
	row.SharedCode = &model.SharedCodeRow{
		Target:  config.SharedCode.Target,
		Scripts: scripts,
	}
	return nil
}
