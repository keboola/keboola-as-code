package jsonnetfiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *jsonNetMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Convert all Json files to JsonNet
	errors := utils.NewMultiError()
	for _, file := range recipe.Files.All() {
		if file.HasTag(model.FileTypeJson) {
			jsonFile := file.File().(*filesystem.JsonFile)

			// Convert
			jsonNetFile, err := jsonFile.ToJsonNetFile()
			if err != nil {
				errors.Append(err)
				continue
			}

			// Replace file
			file.SetFile(jsonNetFile)
			file.RemoveTag(model.FileTypeJson)
			file.AddTag(model.FileTypeJsonNet)
		}
	}
	return errors.ErrorOrNil()
}
