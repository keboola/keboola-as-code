package transformation

import (
	"fmt"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"path/filepath"
)

const (
	blocksDir         = `blocks`
	blockNameTemplate = utils.PathTemplate(`{block_order}-{block_name}`)
	codeNameTemplate  = utils.PathTemplate(`{code_order}-{code_name}`)
)

func RenameBlock(projectDir string, config *manifest.ConfigManifest, index int, block *model.Block) (plans []*model.RenamePlan) {
	// Update parent path
	block.ParentPath = filepath.Join(config.RelativePath(), blocksDir)

	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(projectDir, block.RelativePath())

	// Rename
	block.Path = blockPath(index, block.Name)
	plan.NewPath = filepath.Join(projectDir, block.RelativePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	// Process codes
	for index, code := range block.Codes {
		plans = append(plans, renameCode(projectDir, config.ComponentId, block, index, code)...)
	}

	return plans
}

func renameCode(projectDir, componentId string, block *model.Block, index int, code *model.Code) (plans []*model.RenamePlan) {
	// Update parent path
	code.ParentPath = block.RelativePath()

	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(projectDir, code.RelativePath())

	// Rename
	code.Path = codePath(index, code.Name)
	plan.NewPath = filepath.Join(projectDir, code.RelativePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	// Rename code file
	plans = append(plans, renameCodeFile(projectDir, componentId, code)...)

	return plans
}

func renameCodeFile(projectDir, componentId string, code *model.Code) (plans []*model.RenamePlan) {
	// Store old path
	plan := &model.RenamePlan{}
	plan.OldPath = filepath.Join(projectDir, code.CodeFilePath())

	// Rename
	code.CodeFileName = codeFileName(componentId)
	plan.NewPath = filepath.Join(projectDir, code.CodeFilePath())
	if plan.OldPath != plan.NewPath {
		plans = append(plans, plan)
	}

	return plans
}

func blockPath(index int, name string) string {
	return utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, index+1),
		"block_name":  utils.NormalizeName(name),
	})
}

func codePath(index int, name string) string {
	return utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, index+1),
		"code_name":  utils.NormalizeName(name),
	})
}

func codeFileName(componentId string) string {
	return model.CodeFileName + "." + codeFileExt(componentId)
}

func codeFileExt(componentId string) string {
	switch componentId {
	case `keboola.snowflake-transformation`:
		return `sql`
	case `keboola.synapse-transformation`:
		return `sql`
	case `keboola.oracle-transformation`:
		return `sql`
	case `keboola.r-transformation`:
		return `r`
	case `keboola.julia-transformation`:
		return `jl`
	case `keboola.python-spark-transformation`:
		return `py`
	case `keboola.python-transformation`:
		return `py`
	case `keboola.python-transformation-v2`:
		return `py`
	case `keboola.csas-python-transformation-v2`:
		return `py`
	default:
		return `txt`
	}
}
