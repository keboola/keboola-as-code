package transformation

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// MapBeforeRemoteSave - save code blocks to the API.
func (m *transformationMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.InternalObject); err != nil {
		return err
	} else if !ok {
		return nil
	}
	internalObject := recipe.InternalObject.(*model.Config)
	apiObject := recipe.ApiObject.(*model.Config)

	// Get parameters
	var parameters orderedmap.OrderedMap
	parametersRaw := utils.GetFromMap(apiObject.Content, []string{`parameters`})
	if v, ok := parametersRaw.(orderedmap.OrderedMap); ok {
		parameters = v
	} else {
		parameters = *utils.NewOrderedMap()
	}

	// Convert blocks to map
	blocks := make([]interface{}, 0)
	for _, block := range internalObject.Blocks {
		blockRaw := *utils.NewOrderedMap()
		if err := utils.ConvertByJson(block, &blockRaw); err != nil {
			return fmt.Errorf(`cannot convert block to JSON: %w`, err)
		}
		blocks = append(blocks, blockRaw)
	}

	// Add "parameters.blocks" to configuration content
	parameters.Set("blocks", blocks)

	// Set parameters
	apiObject.Content.Set(`parameters`, parameters)

	// Clear blocks in API object
	apiObject.Blocks = nil

	// Update changed fields
	if recipe.ChangedFields.Has(`blocks`) {
		recipe.ChangedFields.Remove(`blocks`)
		recipe.ChangedFields.Add(`configuration`)
	}

	return nil
}
