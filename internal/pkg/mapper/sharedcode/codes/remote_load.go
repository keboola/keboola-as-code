package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
)

// OnRemoteChange converts legacy "code_content" string -> []interface{}.
func (m *mapper) OnRemoteChange(changes *model.RemoteChanges) error {
	allObjects := m.State.RemoteObjects()
	for _, objectState := range changes.Loaded() {
		// Only for shared code config row
		if ok, err := m.IsSharedCodeRowKey(objectState.Key()); err != nil {
			return err
		} else if ok {
			m.normalizeRemoteSharedCodeRow(objectState.(*model.ConfigRowState).Remote, allObjects)
		}
	}

	return nil
}

func (m *mapper) normalizeRemoteSharedCodeRow(row *model.ConfigRow, allObjects *model.StateObjects) {
	// Get "code_content" value
	raw, found := row.Content.Get(model.SharedCodeContentKey)
	if !found {
		return
	}

	// Convert legacy string -> []interface{}
	if codeContentStr, ok := raw.(string); ok {
		// Get target component of the shared code -> needed for scripts parsing
		config := allObjects.MustGet(row.ConfigKey()).(*model.Config)
		targetComponentId, err := m.GetTargetComponentId(config)
		if err != nil {
			m.Logger.Warn(`Warning: `, err)
			return
		}

		// Parse scripts
		scripts := strhelper.ParseTransformationScripts(codeContentStr, targetComponentId.String())

		// Convert []string -> []interface{}
		scriptsRaw := make([]interface{}, 0)
		for _, script := range scripts {
			scriptsRaw = append(scriptsRaw, script)
		}
		row.Content.Set(model.SharedCodeContentKey, scriptsRaw)
	} else if _, ok := raw.([]interface{}); !ok {
		row.Content.Delete(model.SharedCodeContentKey)
		m.Logger.Warnf(`Warning: key "%s" must be string or string[], found %T, in %s`, model.SharedCodeContentKey, raw, row.Desc())
	}
}
