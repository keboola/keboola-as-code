package codes

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapAfterLocalLoad loads shared code from filesystem to target config.
func (m *mapper) MapAfterLocalLoad(recipe *model.LocalLoadRecipe) error {
	errors := utils.NewMultiError()

	// Shared code config
	if ok, err := m.IsSharedCodeKey(recipe.Object.Key()); err != nil {
		return err
	} else if ok {
		config := recipe.Object.(*model.Config)
		if err := m.onConfigLocalLoad(config); err != nil {
			errors.Append(err)
		}
	}

	// Shared code config row
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil {
		return err
	} else if ok {
		row := recipe.Object.(*model.ConfigRow)
		config := m.state.MustGet(row.ConfigKey()).LocalState().(*model.Config)
		if err := m.onRowLocalLoad(config, row, recipe); err != nil {
			errors.Append(err)
		}
	}

	return errors.ErrorOrNil()
}

func (m *mapper) onConfigLocalLoad(config *model.Config) error {
	// Get "code_content" value
	targetRaw, found := config.Content.Get(model.ShareCodeTargetComponentKey)
	if !found {
		return nil
	}

	// Always delete key from the Content
	defer func() {
		config.Content.Delete(model.ShareCodeTargetComponentKey)
	}()

	// Value should be string
	target, ok := targetRaw.(string)
	if !ok {
		return utils.PrefixError(
			fmt.Sprintf(`invalid %s`, config.Desc()),
			fmt.Errorf(`key "%s" should be string, found "%T"`, model.ShareCodeTargetComponentKey, targetRaw),
		)
	}

	// Store target component ID to struct
	config.SharedCode = &model.SharedCodeConfig{Target: model.ComponentId(target)}
	return nil
}

func (m *mapper) onRowLocalLoad(config *model.Config, row *model.ConfigRow, recipe *model.LocalLoadRecipe) error {
	if config.SharedCode == nil {
		return nil
	}

	// Load file
	codeFile, err := recipe.Files.
		Load(m.state.NamingGenerator().SharedCodeFilePath(recipe.Path(), config.SharedCode.Target)).
		SetDescription("shared code").
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeSharedCode).
		ReadFile()
	if err != nil {
		return err
	}

	// Store scripts to struct
	row.SharedCode = &model.SharedCodeRow{
		Target:  config.SharedCode.Target,
		Scripts: model.ScriptsFromStr(codeFile.Content, config.SharedCode.Target),
	}
	return nil
}
