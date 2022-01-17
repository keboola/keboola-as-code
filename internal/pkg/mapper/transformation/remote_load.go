package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// MapAfterRemoteLoad - load code blocks from API to blocks field.
func (m *transformationMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
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
	var blocks []interface{}
	blocksRaw, _ := parameters.Get(`blocks`)
	if v, ok := blocksRaw.([]interface{}); ok {
		blocks = v
	}

	// Remove blocks from config.json
	parameters.Delete(`blocks`)
	config.Content.Set(`parameters`, parameters)

	// Convert map to Block structs
	config.Transformation = &model.Transformation{}
	if err := json.ConvertByJson(blocks, &config.Transformation.Blocks); err != nil {
		return err
	}

	// Fill in keys
	for blockIndex, block := range config.Transformation.Blocks {
		block.BranchId = config.BranchId
		block.ComponentId = config.ComponentId
		block.ConfigId = config.Id
		block.Index = blockIndex
		for codeIndex, code := range block.Codes {
			code.BranchId = config.BranchId
			code.ComponentId = config.ComponentId
			code.ConfigId = config.Id
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
				code.CodeFileName = m.state.NamingGenerator().CodeFileName(config.ComponentId)
			}
		}
	}

	return nil
}
