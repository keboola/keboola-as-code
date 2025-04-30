package codes

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad loads shared code from filesystem to target config.
func (m *mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	errs := errors.NewMultiError()

	// Shared code config
	if ok, err := m.IsSharedCodeKey(recipe.Object.Key()); err != nil {
		return err
	} else if ok {
		config := recipe.Object.(*model.Config)
		if err := m.onConfigLocalLoad(config); err != nil {
			errs.Append(err)
		}
	}

	// Shared code config row
	if ok, err := m.IsSharedCodeRowKey(recipe.Object.Key()); err != nil {
		return err
	} else if ok {
		row := recipe.Object.(*model.ConfigRow)
		config := m.state.MustGet(row.ConfigKey()).LocalState().(*model.Config)
		if err := m.onRowLocalLoad(ctx, config, row, recipe); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
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
		return errors.NewNestedError(
			errors.Errorf(`invalid %s`, config.Desc()),
			errors.Errorf(`key "%s" should be string, found "%T"`, model.ShareCodeTargetComponentKey, targetRaw),
		)
	}

	// Store target component ID to struct
	config.SharedCode = &model.SharedCodeConfig{Target: keboola.ComponentID(target)}
	return nil
}

func (m *mapper) onRowLocalLoad(ctx context.Context, config *model.Config, row *model.ConfigRow, recipe *model.LocalLoadRecipe) error {
	if config.SharedCode == nil {
		return nil
	}

	// Load file
	codeFile, err := recipe.Files.
		Load(m.state.NamingGenerator().SharedCodeFilePath(recipe.Path(), config.SharedCode.Target)).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription("shared code").
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindNativeSharedCode).
		ReadFile(ctx)
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
