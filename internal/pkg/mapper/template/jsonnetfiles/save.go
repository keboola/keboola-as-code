package jsonnetfiles

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *jsonnetMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Convert all Json files to Jsonnet
	errs := errors.NewMultiError()
	modified := model.NewFilesToSave()
	for _, file := range recipe.Files.All() {
		if file.HasTag(model.FileTypeJSON) {
			jsonFile := file.(*filesystem.JSONFile)

			// Convert
			jsonnetFile, err := jsonFile.ToJsonnetFile()
			if err != nil {
				errs.Append(err)
				continue
			}

			// Replace file
			jsonnetFile.RemoveTag(model.FileTypeJSON)
			jsonnetFile.AddTag(model.FileTypeJsonnet)
			modified.Add(jsonnetFile)
		} else {
			modified.Add(file)
		}
	}

	recipe.Files = modified
	return errs.ErrorOrNil()
}
