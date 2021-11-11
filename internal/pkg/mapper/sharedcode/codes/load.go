package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type loader struct {
	*mapper
	*model.LocalLoadRecipe
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.Error
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

	// Set to config row JSON
	l.configRow.Content.Set(model.ShareCodeContentKey, codeFile.Content)
	normalizeContent(l.configRow.Content)
	return nil
}
