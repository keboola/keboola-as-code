package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type loader struct {
	*mapper
	*model.LocalLoadRecipe
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.MultiError
}

// OnObjectsLoad converts legacy "code_content" string -> []interface{}.
func (m *mapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	// Only remote load
	if event.StateType != model.StateTypeRemote {
		return nil
	}

	for _, object := range event.NewObjects {
		// Only for shared code config row
		if ok, err := m.IsSharedCodeRowKey(object.Key()); err != nil {
			return err
		} else if ok {
			m.normalizeRemoteSharedCodeRow(object.(*model.ConfigRow), event)
		}
	}

	return nil
}

func (m *mapper) normalizeRemoteSharedCodeRow(row *model.ConfigRow, event model.OnObjectsLoadEvent) {
	// Get "code_content" value
	raw, found := row.Content.Get(model.SharedCodeContentKey)
	if !found {
		return
	}

	// Convert legacy string -> []interface{}
	if codeContentStr, ok := raw.(string); ok {
		// Get target component of the shared code -> needed for scripts parsing
		config := event.AllObjects.MustGet(row.ConfigKey()).(*model.Config)
		targetComponentId, err := m.GetTargetComponentId(config)
		if err != nil {
			m.Logger.Warn(`Warning: `, err)
			return
		}

		// Parse scripts
		scripts := strhelper.ParseTransformationScripts(codeContentStr, targetComponentId)

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

// MapAfterLocalLoad - load shared code from filesystem to target config.
func (m *mapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	// Only for shared code config row
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}

	// Create loader
	configRow := recipe.Object.(*model.ConfigRow)
	config := m.State.MustGet(configRow.ConfigKey()).LocalState().(*model.Config)
	l := &loader{
		mapper:          m,
		LocalLoadRecipe: recipe,
		config:          config,
		configRow:       configRow,
		errors:          utils.NewMultiError(),
	}

	// Load
	return l.load()
}

func (l *loader) load() error {
	// Get target component of the shared code -> needed for file extension
	targetComponentId, err := l.GetTargetComponentId(l.config)
	if err != nil {
		return err
	}

	// Load file
	codeFilePath := l.Naming.SharedCodeFilePath(l.Record.Path(), targetComponentId)
	codeFile, err := l.Fs.ReadFile(codeFilePath, `shared code`)
	if err != nil {
		return err
	}
	l.Record.AddRelatedPath(codeFilePath)

	// Convert []string -> []interface{} (so there is no type difference against API type)
	scripts := strhelper.ParseTransformationScripts(codeFile.Content, targetComponentId)
	scriptsRaw := make([]interface{}, 0)
	for _, script := range scripts {
		scriptsRaw = append(scriptsRaw, script)
	}

	// Set to config row JSON
	l.configRow.Content.Set(model.SharedCodeContentKey, scriptsRaw)
	return nil
}
