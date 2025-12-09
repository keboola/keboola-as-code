package transformation

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
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

	// Get parameters
	parameters, _, _ := config.Content.GetNestedMap(`parameters`)
	if parameters == nil {
		// Create if not found or has invalid type
		parameters = orderedmap.New()
		config.Content.Set(`parameters`, parameters)
	}

	// Get blocks
	var blocks []any
	blocksRaw, _ := parameters.Get(`blocks`)
	if v, ok := blocksRaw.([]any); ok {
		blocks = v
	}

	// Normalize block field order: ensure "name" comes before "codes"
	// This fixes the order when blocks come from Jsonnet (which outputs fields alphabetically)
	for _, blockRaw := range blocks {
		if blockMap, ok := blockRaw.(*orderedmap.OrderedMap); ok {
			// Get current values
			name, hasName := blockMap.Get("name")
			codes, hasCodes := blockMap.Get("codes")

			// Recreate with correct order
			if hasName && hasCodes {
				normalizedBlock := orderedmap.New()
				normalizedBlock.Set("name", name)
				normalizedBlock.Set("codes", codes)
				// Copy any other fields
				for _, key := range blockMap.Keys() {
					if key != "name" && key != "codes" {
						if val, ok := blockMap.Get(key); ok {
							normalizedBlock.Set(key, val)
						}
					}
				}
				// Replace in parameters
				*blockMap = *normalizedBlock
			}
		}
	}
	// Update parameters with normalized blocks before removing
	parameters.Set("blocks", blocks)
	config.Content.Set(`parameters`, parameters)

	// Remove blocks from config.json
	parameters.Delete(`blocks`)
	config.Content.Set(`parameters`, parameters)

	// Convert map to Block structs
	config.Transformation = &model.Transformation{}
	if err := json.ConvertByJSON(blocks, &config.Transformation.Blocks); err != nil {
		return err
	}

	// Fill in keys
	for blockIndex, block := range config.Transformation.Blocks {
		block.BranchID = config.BranchID
		block.ComponentID = config.ComponentID
		block.ConfigID = config.ID
		block.Index = blockIndex
		for codeIndex, code := range block.Codes {
			code.BranchID = config.BranchID
			code.ComponentID = config.ComponentID
			code.ConfigID = config.ID
			code.BlockIndex = block.Index
			code.Index = codeIndex
			for _, script := range code.Scripts {
				if v, ok := script.(model.StaticScript); ok {
					v.Value = model.NormalizeScript(v.Value)
				}
			}
		}
	}

	// Set paths if parent path is set
	if recipe.Path() != "" {
		blocksDir := m.state.NamingGenerator().BlocksDir(recipe.Path())
		for _, block := range config.Transformation.Blocks {
			if path, found := m.state.GetPath(block.Key()); found {
				block.AbsPath = path
			} else {
				block.AbsPath = m.state.NamingGenerator().BlockPath(blocksDir, block)
			}
			for _, code := range block.Codes {
				if path, found := m.state.GetPath(code.Key()); found {
					code.AbsPath = path
				} else {
					code.AbsPath = m.state.NamingGenerator().CodePath(block.Path(), code)
				}
				code.CodeFileName = m.state.NamingGenerator().CodeFileName(config.ComponentID)
			}
		}
	}

	return nil
}
