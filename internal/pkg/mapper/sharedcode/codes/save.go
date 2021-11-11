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
	errors    *utils.Error
}

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
	raw, found := rowContent.Get(model.ShareCodeContentKey)
	if !found {
		return fmt.Errorf(`key "%s" not found in %s`, model.ShareCodeContentKey, w.configRow.Desc())
	}

	// Get target component of the shared code -> needed for file extension
	targetComponentId, err := w.GetTargetComponentId(w.config)
	if err != nil {
		return err
	}

	// Content must be string or array
	var codeContent string
	if codeContentStr, ok := raw.(string); ok {
		codeContent = strhelper.TransformationScriptsToString([]string{codeContentStr}, targetComponentId)
	} else if codeContentSlice, ok := raw.([]interface{}); ok {
		var scripts []string
		for _, script := range codeContentSlice {
			scripts = append(scripts, cast.ToString(script))
		}
		codeContent = strhelper.TransformationScriptsToString(scripts, targetComponentId)
	} else {
		return fmt.Errorf(`key "%s" must be string or string[], found %T, in %s`, model.ShareCodeContentKey, raw, w.configRow.Desc())
	}

	// Remove code content from JSON
	rowContent.Delete(model.ShareCodeContentKey)

	// Generate code file
	codeFilePath := w.Naming.SharedCodeFilePath(w.Path(), targetComponentId)
	codeFile := filesystem.CreateFile(codeFilePath, codeContent).SetDescription(`shared code`)
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
