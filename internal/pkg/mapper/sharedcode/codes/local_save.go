package codes

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeLocalSave saves shared code as native file to filesystem.
func (m *mapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Save config
	if config, ok := recipe.Object.(*model.Config); ok {
		m.onConfigLocalSave(config)
	}

	// Save row
	if row, ok := recipe.Object.(*model.ConfigRow); ok {
		m.onRowLocalSave(row, recipe)
	}

	return nil
}

func (m *mapper) onConfigLocalSave(config *model.Config) {
	// Is shared code?
	if config.SharedCode == nil {
		return
	}

	// Set target component ID
	config.Content.Set(model.ShareCodeTargetComponentKey, config.SharedCode.Target.String())
}

func (m *mapper) onRowLocalSave(row *model.ConfigRow, recipe *model.LocalSaveRecipe) {
	// Is shared code?
	if row.SharedCode == nil {
		return
	}

	// Create code file
	codeContent := row.SharedCode.String()
	codeFilePath := m.state.NamingGenerator().SharedCodeFilePath(recipe.Path(), row.SharedCode.Target)
	recipe.Files.
		Add(filesystem.NewFile(codeFilePath, codeContent).SetDescription(`shared code`)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeSharedCode)

	// Remove "isDisabled" unnecessary value from "meta.json".
	// Shared code is represented as config row
	// and always contains `"isDisabled": false` in metadata.
	if !row.IsDisabled {
		// isDisabled == false -> hide "isDisabled" field in meta.json
		fields, _ := recipe.Annotations[corefiles.HideMetaFileFieldsAnnotation].([]string)
		recipe.Annotations[corefiles.HideMetaFileFieldsAnnotation] = append(fields, `isDisabled`)
	}
}
