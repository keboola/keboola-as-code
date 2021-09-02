package model

import (
	"fmt"
	"path/filepath"

	"keboola-as-code/src/utils"
)

const (
	MetaFile          = "meta.json"
	ConfigFile        = "config.json"
	CodeFileName      = `code` // transformation code block name without ext
	blocksDir         = `blocks`
	blockNameTemplate = utils.PathTemplate(`{block_order}-{block_name}`)
	codeNameTemplate  = utils.PathTemplate(`{code_order}-{code_name}`)
	SqlExt            = `sql`
	PyExt             = `py`
	JuliaExt          = `jl`
	RExt              = `r`
	TxtExt            = `txt`
)

// Naming of the files.
type Naming struct {
	Branch    utils.PathTemplate `json:"branch" validate:"required"`
	Config    utils.PathTemplate `json:"config" validate:"required"`
	ConfigRow utils.PathTemplate `json:"configRow" validate:"required"`
}

func DefaultNaming() Naming {
	return Naming{
		Branch:    "{branch_id}-{branch_name}",
		Config:    "{component_type}/{component_id}/{config_id}-{config_name}",
		ConfigRow: "rows/{config_row_id}-{config_row_name}",
	}
}

func (n Naming) MetaFilePath(dir string) string {
	return filepath.Join(dir, MetaFile)
}

func (n Naming) ConfigFilePath(dir string) string {
	return filepath.Join(dir, ConfigFile)
}

func (n Naming) BranchPath(branch *Branch, isDefault bool) string {
	if isDefault {
		return `main`
	}

	return utils.ReplacePlaceholders(string(n.Branch), map[string]interface{}{
		"branch_id":   branch.Id,
		"branch_name": utils.NormalizeName(branch.Name),
	})
}

func (n Naming) ConfigPath(component *Component, config *Config) string {
	return utils.ReplacePlaceholders(string(n.Config), map[string]interface{}{
		"component_type": component.Type,
		"component_id":   component.Id,
		"config_id":      config.Id,
		"config_name":    utils.NormalizeName(config.Name),
	})
}

func (n Naming) ConfigRowPath(row *ConfigRow) string {
	return utils.ReplacePlaceholders(string(n.ConfigRow), map[string]interface{}{
		"config_row_id":   row.Id,
		"config_row_name": utils.NormalizeName(row.Name),
	})
}

func (n Naming) BlocksDir(configDir string) string {
	return filepath.Join(configDir, blocksDir)
}

func (n Naming) BlocksTmpDir(configDir string) string {
	return filepath.Join(configDir, `.new_`+blocksDir)
}

func (n Naming) BlockPath(index int, name string) string {
	return utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, index+1),
		"block_name":  utils.NormalizeName(name),
	})
}

func (n Naming) CodePath(index int, name string) string {
	return utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, index+1),
		"code_name":  utils.NormalizeName(name),
	})
}

func (n Naming) CodeFilePath(code *Code) string {
	return filepath.Join(code.RelativePath(), code.CodeFileName)
}

func (n Naming) CodeFileName(componentId string) string {
	return CodeFileName + "." + n.CodeFileExt(componentId)
}

func (n Naming) CodeFileExt(componentId string) string {
	switch componentId {
	case `keboola.snowflake-transformation`:
		return SqlExt
	case `keboola.synapse-transformation`:
		return SqlExt
	case `keboola.oracle-transformation`:
		return SqlExt
	case `keboola.r-transformation`:
		return RExt
	case `keboola.julia-transformation`:
		return JuliaExt
	case `keboola.python-spark-transformation`:
		return PyExt
	case `keboola.python-transformation`:
		return PyExt
	case `keboola.python-transformation-v2`:
		return PyExt
	case `keboola.csas-python-transformation-v2`:
		return PyExt
	default:
		return TxtExt
	}
}
