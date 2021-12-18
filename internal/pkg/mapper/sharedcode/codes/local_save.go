package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave saves shared code as native file to filesystem.
func (m *mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Save config
	if config, ok := recipe.Object.(*model.Config); ok {
		m.onConfigLocalSave(config, recipe)
	}

	// Save row
	if row, ok := recipe.Object.(*model.ConfigRow); ok {
		m.onRowLocalSave(row, recipe)
	}

	return nil
}

func (m *mapper) onConfigLocalSave(config *model.Config, recipe *model.LocalSaveRecipe) {
	// Is shared code?
	if config.SharedCode == nil {
		return
	}

	// Get config file
	configFile, err := recipe.Files.ObjectConfigFile()
	if err != nil {
		panic(err)
	}

	// Set target component ID
	configFile.Content.Set(model.ShareCodeTargetComponentKey, config.SharedCode.Target.String())
}

func (m *mapper) onRowLocalSave(row *model.ConfigRow, recipe *model.LocalSaveRecipe) {
	// Is shared code?
	if row.SharedCode == nil {
		return
	}

	// Create code file
	codeContent := row.SharedCode.String()
	codeFilePath := m.NamingGenerator.SharedCodeFilePath(recipe.Path(), row.SharedCode.Target)
	recipe.Files.
		Add(filesystem.NewFile(codeFilePath, codeContent).SetDescription(`shared code`)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeSharedCode)

	// Remove "isDisabled" unnecessary value from "meta.json".
	// Shared code is represented as config row
	// and always contains `"isDisabled": false` in metadata.
	metaFile, err := recipe.Files.ObjectMetaFile()
	if err != nil {
		panic(err)
	}
	if value, found := metaFile.Content.Get(`isDisabled`); found {
		if v, ok := value.(bool); ok && !v {
			// Found `"isDisabled": false` -> delete
			metaFile.Content.Delete(`isDisabled`)
		}
	}
}
