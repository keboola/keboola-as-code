package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave saves shared code target component and code to Content.
func (m *mapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Save config
	if config, ok := recipe.Object.(*model.Config); ok {
		m.onConfigRemoteSave(config, recipe)
	}

	// Save row
	if row, ok := recipe.Object.(*model.ConfigRow); ok {
		m.onRowRemoteSave(row, recipe)
	}

	return nil
}

func (m *mapper) onConfigRemoteSave(config *model.Config, recipe *model.RemoteSaveRecipe) {
	// Is shared code?
	if config.SharedCode == nil {
		return
	}

	// Set target component ID
	config.Content.Set(model.ShareCodeTargetComponentKey, config.SharedCode.Target.String())

	// Update changed fields
	if recipe.ChangedFields.Has(`sharedCode`) {
		recipe.ChangedFields.Remove(`sharedCode`)
		recipe.ChangedFields.Add(`configuration`)
	}
}

func (m *mapper) onRowRemoteSave(row *model.ConfigRow, recipe *model.RemoteSaveRecipe) {
	// Is shared code?
	if row.SharedCode == nil {
		return
	}

	// Set target component ID
	row.Content.Set(model.SharedCodeContentKey, row.SharedCode.Scripts.Slice())

	// Update changed fields
	if recipe.ChangedFields.Has(`sharedCode`) {
		recipe.ChangedFields.Remove(`sharedCode`)
		recipe.ChangedFields.Add(`configuration`)
	}
}
