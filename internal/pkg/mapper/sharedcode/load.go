package sharedcode

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type loader struct {
	model.MapperContext
	*model.LocalLoadRecipe
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.Error
}

// MapAfterRemoteLoad - extract shared code "variables_id".
func (m *sharedCodeMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Variables are used by shared code - config row.
	apiObject, ok := recipe.ApiObject.(*model.ConfigRow)
	if !ok {
		return nil
	}
	internalObject := recipe.InternalObject.(*model.ConfigRow)

	// Check component type
	component, err := m.State.Components().Get(internalObject.ComponentKey())
	if err != nil {
		return err
	}
	if !component.IsSharedCode() {
		return nil
	}

	// Variables ID is stored in configuration
	variablesIdRaw, found := apiObject.Content.Get(model.SharedCodeVariablesIdContentKey)
	if !found {
		return nil
	}

	// Variables ID must be string
	variablesId, ok := variablesIdRaw.(string)
	if !ok {
		return nil
	}

	// Create relation
	internalObject.AddRelation(&model.SharedCodeVariablesFromRelation{
		VariablesId: variablesId,
	})

	// Remove variables ID from configuration content
	internalObject.Content.Delete(model.SharedCodeVariablesIdContentKey)
	return nil
}

// MapAfterLocalLoad - load shared code from filesystem to target config.
func (m *sharedCodeMapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	// Only for shared code config row
	if ok, err := m.isSharedCodeConfigRow(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}

	// Create loader
	configRow := recipe.Object.(*model.ConfigRow)
	config := m.State.MustGet(configRow.ConfigKey()).LocalState().(*model.Config)
	l := &loader{
		MapperContext:   m.MapperContext,
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
	targetComponentId, err := getTargetComponentId(l.config)
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
