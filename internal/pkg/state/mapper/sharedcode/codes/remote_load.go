package codes

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// AfterRemoteOperation converts legacy "code_content" string -> []interface{}.
func (m *remoteMapper) AfterRemoteOperation(changes *model.Changes) error {
	errors := utils.NewMultiError()
	var configs []*model.Config
	var rows []*model.ConfigRow

	// Get all new loaded shared code configs and rows
	for _, object := range changes.Loaded() {
		if config, ok := object.(*model.Config); ok {
			if ok, err := m.IsSharedCodeKey(config.Key()); err != nil {
				errors.Append(err)
				continue
			} else if ok {
				configs = append(configs, config)
			}
		} else if row, ok := object.(*model.ConfigRow); ok {
			if ok, err := m.IsSharedCodeKey(row.ConfigKey()); err != nil {
				errors.Append(err)
				continue
			} else if ok {
				rows = append(rows, row)
			}
		}
	}

	// Process configs first
	for _, config := range configs {
		if err := m.onConfigRemoteLoad(config); err != nil {
			errors.Append(err)
		}
	}

	// Process rows
	for _, row := range rows {
		if err := m.onRowRemoteLoad(row); err != nil {
			errors.Append(err)
		}
	}

	if errors.Len() > 0 {
		// Convert errors to warning
		m.logger.Warn(utils.PrefixError(`Warning`, errors))
	}

	return nil
}

func (m *remoteMapper) onConfigRemoteLoad(config *model.Config) error {
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
			fmt.Sprintf(`invalid %s`, config.String()),
			fmt.Errorf(`key "%s" should be string, found "%T"`, model.ShareCodeTargetComponentKey, targetRaw),
		)
	}

	// Store target component ID to struct
	config.SharedCode = &model.SharedCodeConfig{Target: model.ComponentId(target)}
	return nil
}

func (m *remoteMapper) onRowRemoteLoad(row *model.ConfigRow) error {
	// Get "code_content" value
	raw, found := row.Content.Get(model.SharedCodeContentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		row.Content.Delete(model.SharedCodeContentKey)
	}()

	// Get config
	config := m.state.MustGet(row.ConfigKey()).(*model.Config)

	// Parse value
	var scripts model.Scripts
	switch v := raw.(type) {
	case string:
		scripts = model.ScriptsFromStr(v, config.SharedCode.Target)
	case []interface{}:
		scripts = model.ScriptsFromSlice(v)
	default:
		return utils.PrefixError(
			fmt.Sprintf(`invalid %s`, row.String()),
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
