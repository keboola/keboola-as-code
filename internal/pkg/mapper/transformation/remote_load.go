package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/strhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// MapAfterRemoteLoad - load code blocks from API to blocks field.
func (m *transformationMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	// Only for transformation config
	if ok, err := m.isTransformationConfig(recipe.InternalObject); err != nil {
		return err
	} else if !ok {
		return nil
	}
	config := recipe.InternalObject.(*model.Config)

	// Get parameters
	var parameters *orderedmap.OrderedMap
	parametersRaw := utils.GetFromMap(config.Content, []string{`parameters`})
	if v, ok := parametersRaw.(*orderedmap.OrderedMap); ok {
		parameters = v
	} else {
		parameters = utils.NewOrderedMap()
	}

	// Get blocks
	var blocks []interface{}
	blocksRaw := utils.GetFromMap(parameters, []string{`blocks`})
	if v, ok := blocksRaw.([]interface{}); ok {
		blocks = v
	}

	// Remove blocks from config.json
	parameters.Delete(`blocks`)
	config.Content.Set(`parameters`, parameters)

	// Convert map to Block structs
	if err := utils.ConvertByJson(blocks, &config.Blocks); err != nil {
		return err
	}

	// Fill in keys
	for blockIndex, block := range config.Blocks {
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
			for i, script := range code.Scripts {
				code.Scripts[i] = strhelper.NormalizeScript(script)
			}
		}
	}

	// Set paths if parent path is set
	if recipe.Manifest.Path() != "" {
		blocksDir := m.Naming.BlocksDir(recipe.Manifest.Path())
		for _, block := range config.Blocks {
			if path, found := m.Naming.GetCurrentPath(block.Key()); found {
				block.PathInProject = path
			} else {
				block.PathInProject = m.Naming.BlockPath(blocksDir, block)
			}
			for _, code := range block.Codes {
				if path, found := m.Naming.GetCurrentPath(code.Key()); found {
					code.PathInProject = path
				} else {
					code.PathInProject = m.Naming.CodePath(block.Path(), code)
				}
				code.CodeFileName = m.Naming.CodeFileName(config.ComponentId)
			}
		}
	}

	return nil
}
