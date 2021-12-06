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
	codeFilePath := l.Naming.SharedCodeFilePath(l.ObjectManifest.Path(), targetComponentId)
	codeFile, err := l.Fs.ReadFile(codeFilePath, `shared code`)
	if err != nil {
		return err
	}
	l.Files.
		Add(codeFile).
		AddTag(model.FileTypeNativeSharedCode)

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
