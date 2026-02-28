package notification

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// MapAfterLocalLoad reads notification config.json and registers meta.json.
func (m *mapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	notification, ok := recipe.Object.(*model.Notification)
	if !ok {
		return nil
	}

	errs := errors.NewMultiError()
	if err := m.loadConfigFile(ctx, recipe, notification); err != nil {
		errs.Append(err)
	}
	if err := m.loadMetaFile(ctx, recipe); err != nil {
		errs.Append(err)
	}
	return errs.ErrorOrNil()
}

func (m *mapper) loadConfigFile(ctx context.Context, recipe *model.LocalLoadRecipe, notification *model.Notification) error {
	_, err := recipe.Files.
		Load(m.state.NamingGenerator().ConfigFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectConfig).
		ReadJSONFileTo(ctx, notification)
	return err
}

func (m *mapper) loadMetaFile(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	_, err := recipe.Files.
		Load(m.state.NamingGenerator().MetaFilePath(recipe.ObjectManifest.Path())).
		AddMetadata(filesystem.ObjectKeyMetadata, recipe.Key()).
		SetDescription(recipe.ObjectManifest.Kind().Name + " metadata").
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindObjectMeta).
		ReadJSONFile(ctx)
	return err
}
