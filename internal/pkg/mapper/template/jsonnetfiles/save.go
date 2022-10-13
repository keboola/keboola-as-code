package jsonnetfiles

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *jsonNetMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Convert all Json files to JsonNet
	errs := errors.NewMultiError()
	modified := model.NewFilesToSave()
	for _, file := range recipe.Files.All() {
		if file.HasTag(model.FileTypeJson) {
			jsonFile := file.(*filesystem.JsonFile)

			// Convert
			jsonNetFile, err := jsonFile.ToJsonNetFile()
			if err != nil {
				errs.Append(err)
				continue
			}

			// Replace file
			jsonNetFile.RemoveTag(model.FileTypeJson)
			jsonNetFile.AddTag(model.FileTypeJsonNet)
			modified.Add(jsonNetFile)
		} else {
			modified.Add(file)
		}
	}

	recipe.Files = modified
	return errs.ErrorOrNil()
}
