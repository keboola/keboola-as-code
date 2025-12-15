package transformation

import (
	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type transformationMapper struct {
	state  *state.State
	logger log.Logger
}

func NewMapper(s *state.State) *transformationMapper {
	return &transformationMapper{state: s, logger: s.Logger()}
}

func (m *transformationMapper) isTransformationConfig(object any) (bool, error) {
	v, ok := object.(*model.Config)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(v.ComponentID)
	if err != nil {
		return false, err
	}

	return component.IsTransformationWithBlocks(), nil
}

func (m *transformationMapper) isTransformationConfigState(objectState model.ObjectState) (bool, error) {
	v, ok := objectState.(*model.ConfigState)
	if !ok {
		return false, nil
	}

	component, err := m.state.Components().GetOrErr(v.ComponentID)
	if err != nil {
		return false, err
	}

	return component.IsTransformationWithBlocks(), nil
}

// ParseBlocksFromContent parses blocks from config.Content.parameters.blocks and populates config.Transformation.
// This is used both when loading from remote API and when creating new configs from schema.
func (m *transformationMapper) ParseBlocksFromContent(config *model.Config, configPath string) error {
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

	// Update parameters with normalized blocks
	// For Python transformations, keep blocks in config.json for JSON schema validation
	// For other transformations (like Snowflake), remove blocks from config.json
	if config.ComponentID == "keboola.python-transformation-v2" {
		parameters.Set("blocks", blocks)
	} else {
		parameters.Delete("blocks")
	}
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
	if configPath != "" {
		blocksDir := m.state.NamingGenerator().BlocksDir(configPath)
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
