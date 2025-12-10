package transformation

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave - save code blocks to the API.
func (m *transformationMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.Object); err != nil {
		return err
	} else if !ok {
		return nil
	}
	apiObject := recipe.Object.(*model.Config)

	// Get parameters
	parameters, _, _ := apiObject.Content.GetNestedMap(`parameters`)
	if parameters == nil {
		// Create if not found or has invalid type
		parameters = orderedmap.New()
		apiObject.Content.Set(`parameters`, parameters)
	}

	// Convert blocks to map
	blocks := make([]any, 0)
	for _, block := range apiObject.Transformation.Blocks {
		blockRaw := orderedmap.New()
		// Set fields in the order they appear in the struct definition
		blockRaw.Set("name", block.Name)
		blockRaw.Set("codes", block.Codes)
		blocks = append(blocks, blockRaw)
	}

	// Add "parameters.blocks" to configuration content
	parameters.Set("blocks", blocks)

	// Clear transformation in API object
	apiObject.Transformation = nil

	// Update changed fields
	if recipe.ChangedFields.Has(`transformation`) {
		recipe.ChangedFields.Remove(`transformation`)
		recipe.ChangedFields.Add(`configuration`)
	}

	return nil
}
