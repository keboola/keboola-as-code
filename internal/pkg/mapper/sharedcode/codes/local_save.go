package codes

import (
	"fmt"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type writer struct {
	*mapper
	*model.LocalSaveRecipe
	config    *model.Config
	configRow *model.ConfigRow
	errors    *utils.MultiError
}

// MapBeforeLocalSave - save shared code as native file to filesystem.
func (m *mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Only for shared code config row
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}

	// Create writer
	configRow := recipe.Object.(*model.ConfigRow)
	config := m.State.MustGet(configRow.ConfigKey()).RemoteOrLocalState().(*model.Config)
	w := &writer{
		mapper:          m,
		LocalSaveRecipe: recipe,
		config:          config,
		configRow:       configRow,
		errors:          utils.NewMultiError(),
	}

	// Save
	return w.save()
}

func (w *writer) save() error {
	// Load content from config row JSON
	rowContent := w.configRow.Content

	// Load content
	raw, found := rowContent.Get(model.SharedCodeContentKey)
	if !found {
		return fmt.Errorf(`key "%s" not found in %s`, model.SharedCodeContentKey, w.configRow.Desc())
	}

	// Get target component of the shared code -> needed for file extension
	targetComponentId, err := w.GetTargetComponentId(w.config)
	if err != nil {
		return err
	}

	// Content must be []interface{}
	codeContentSlice, ok := raw.([]interface{})
	if !ok {
		return fmt.Errorf(`key "%s" must be array, found %T, in %s`, model.SharedCodeContentKey, raw, w.configRow.Desc())
	}

	// Remove code content from JSON
	w.Configuration.Content.Delete(model.SharedCodeContentKey)

	// Convert []interface{} -> []string
	var scripts []string
	for _, script := range codeContentSlice {
		scripts = append(scripts, cast.ToString(script))
	}

	// Generate file content
	codeContent := strhelper.TransformationScriptsToString(scripts, targetComponentId)

	// Create code file
	codeFilePath := w.Naming.SharedCodeFilePath(w.Path(), targetComponentId)
	codeFile := filesystem.NewFile(codeFilePath, codeContent).SetDescription(`shared code`)
	w.ExtraFiles = append(w.ExtraFiles, codeFile)

	// Remove "isDisabled" unnecessary value from "meta.json".
	// Shared code is represented as config row
	// and always contains `"isDisabled": false` in metadata.
	meta := w.Metadata
	if meta != nil && meta.Content != nil {
		if value, found := meta.Content.Get(`isDisabled`); found {
			if v, ok := value.(bool); ok && !v {
				// Found `"isDisabled": false` -> delete
				meta.Content.Delete(`isDisabled`)
			}
		}
	}

	return nil
}
