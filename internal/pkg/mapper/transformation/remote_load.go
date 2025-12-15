package transformation

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapAfterRemoteLoad - load code blocks from API to blocks field.
func (m *transformationMapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}
	config := recipe.Object.(*model.Config)

	// Parse blocks from config content
	return m.ParseBlocksFromContent(config, recipe.Path())
}
