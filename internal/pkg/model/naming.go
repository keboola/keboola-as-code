package model

import (
	"fmt"
	"sync"

	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	MetaFile          = "meta.json"
	ConfigFile        = "config.json"
	DescriptionFile   = "description.md"
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
	Branch              utils.PathTemplate `json:"branch" validate:"required"`
	Config              utils.PathTemplate `json:"config" validate:"required"`
	ConfigRow           utils.PathTemplate `json:"configRow" validate:"required"`
	SharedCodeConfig    utils.PathTemplate `json:"sharedCodeConfig" validate:"required"`
	SharedCodeConfigRow utils.PathTemplate `json:"sharedCodeConfigRow" validate:"required"`
	Variables           utils.PathTemplate `json:"variables" validate:"required"`
	VariablesValues     utils.PathTemplate `json:"variablesValues" validate:"required"`
	usedLock            *sync.Mutex
	usedByPath          map[string]string // path -> object key
	usedByKey           map[string]string // object key -> path
}

func DefaultNaming() Naming {
	return Naming{
		Branch:              "{branch_id}-{branch_name}",
		Config:              "{component_type}/{component_id}/{config_id}-{config_name}",
		ConfigRow:           "rows/{config_row_id}-{config_row_name}",
		SharedCodeConfig:    "_shared/{target_component_id}",
		SharedCodeConfigRow: "codes/{config_row_id}-{config_row_name}",
		Variables:           "variables",
		VariablesValues:     "values/{config_row_name}",
		usedLock:            &sync.Mutex{},
		usedByPath:          make(map[string]string),
		usedByKey:           make(map[string]string),
	}
}

// Attach object's path to Naming, it guarantees the path will remain unique and will not be used again.
func (n Naming) Attach(key Key, path string) {
	n.usedLock.Lock()
	defer n.usedLock.Unlock()

	// Check if the path is unique
	keyStr := key.String()
	if foundKey, found := n.usedByPath[path]; found && foundKey != keyStr {
		panic(fmt.Errorf(
			`naming error: path "%s" is attached to object "%s", but new object "%s" has same path`,
			path, foundKey, keyStr,
		))
	}

	// Remove the previous value attached to the key
	if foundPath, found := n.usedByKey[keyStr]; found {
		delete(n.usedByPath, foundPath)
	}

	n.usedByPath[path] = keyStr
	n.usedByKey[keyStr] = path
}

// Detach object's path from Naming, so it can be used by other object.
func (n Naming) Detach(key Key) {
	n.usedLock.Lock()
	defer n.usedLock.Unlock()

	if foundPath, found := n.usedByKey[key.String()]; found {
		delete(n.usedByPath, foundPath)
		delete(n.usedByKey, key.String())
	}
}

func (n Naming) ensureUniquePath(key Key, p PathInProject) PathInProject {
	p = n.makeUniquePath(key, p)
	n.Attach(key, p.Path())
	return p
}

func (n Naming) makeUniquePath(key Key, p PathInProject) PathInProject {
	n.usedLock.Lock()
	defer n.usedLock.Unlock()

	// Object path cannot be empty
	if len(p.ObjectPath) == 0 {
		p.ObjectPath = utils.NormalizeName(key.Kind().Name)
	}

	keyStr := key.String()
	dir, file := filesystem.Split(p.ObjectPath)

	// Add a suffix to the path if it is not unique
	suffix := 0
	for {
		foundKey, found := n.usedByPath[p.Path()]
		if !found || foundKey == keyStr {
			break
		}

		suffix++
		p.ObjectPath = filesystem.Join(dir, utils.NormalizeName(file+"-"+fmt.Sprintf(`%03d`, suffix)))
	}
	return p
}

func (n Naming) MetaFilePath(dir string) string {
	return filesystem.Join(dir, MetaFile)
}

func (n Naming) ConfigFilePath(dir string) string {
	return filesystem.Join(dir, ConfigFile)
}

func (n Naming) DescriptionFilePath(dir string) string {
	return filesystem.Join(dir, DescriptionFile)
}

func (n Naming) BranchPath(branch *Branch) PathInProject {
	p := PathInProject{}
	p.ParentPath = "" // branch is top level object

	if branch.IsDefault {
		p.ObjectPath = `main`
	} else {
		p.ObjectPath = utils.ReplacePlaceholders(string(n.Branch), map[string]interface{}{
			"branch_id":   branch.Id,
			"branch_name": utils.NormalizeName(branch.Name),
		})
	}

	return n.ensureUniquePath(branch.Key(), p)
}

func (n Naming) ConfigPath(parentPath string, component *Component, config *Config) PathInProject {
	if len(parentPath) == 0 {
		panic(fmt.Errorf(`config "%s" parent path cannot be empty"`, config))
	}

	// Shared code is handled differently
	var template, targetComponentId string
	if component.IsSharedCode() {
		// Get target component ID for shared code config
		if config.Content == nil {
			panic(fmt.Errorf(`shared code config "%s" must have set key "%s"`, config.Desc(), ShareCodeTargetComponentKey))
		}
		targetComponentIdRaw, found := config.Content.Get(ShareCodeTargetComponentKey)
		if !found {
			panic(fmt.Errorf(`shared code config "%s" must have set key "%s"`, config.Desc(), ShareCodeTargetComponentKey))
		}
		// Shared code
		template = string(n.SharedCodeConfig)
		targetComponentId = cast.ToString(targetComponentIdRaw)
	} else {
		// Ordinary config
		template = string(n.Config)
	}

	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(template, map[string]interface{}{
		"target_component_id": targetComponentId, // for shared code
		"component_type":      component.Type,
		"component_id":        component.Id,
		"config_id":           config.Id,
		"config_name":         utils.NormalizeName(config.Name),
	})
	return n.ensureUniquePath(config.Key(), p)
}

func (n Naming) ConfigRowPath(parentPath string, component *Component, row *ConfigRow) PathInProject {
	if len(parentPath) == 0 {
		panic(fmt.Errorf(`config row "%s" parent path cannot be empty"`, row))
	}

	// Shared code is handled differently
	var template string
	if component.IsSharedCode() {
		template = string(n.SharedCodeConfigRow)
	} else {
		template = string(n.ConfigRow)
	}

	// Row name can be empty.
	name := row.Name
	if len(name) == 0 {
		// Get name from the configuration content (legacy transformations)
		contentName, _ := row.Content.Get("name")
		if contentNameStr, found := contentName.(string); found {
			name = contentNameStr
		} else {
			// Generate name
			name = "row"
		}
	}

	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(template, map[string]interface{}{
		"config_row_id":   row.Id,
		"config_row_name": utils.NormalizeName(name),
	})
	return n.ensureUniquePath(row.Key(), p)
}

func (n Naming) BlocksDir(configDir string) string {
	return filesystem.Join(configDir, blocksDir)
}

func (n Naming) BlocksTmpDir(configDir string) string {
	return filesystem.Join(configDir, `.new_`+blocksDir)
}

func (n Naming) BlockPath(parentPath string, block *Block) PathInProject {
	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, block.Index+1),
		"block_name":  utils.NormalizeName(block.Name),
	})
	return n.ensureUniquePath(block.Key(), p)
}

func (n Naming) CodePath(parentPath string, code *Code) PathInProject {
	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, code.Index+1),
		"code_name":  utils.NormalizeName(code.Name),
	})
	return n.ensureUniquePath(code.Key(), p)
}

func (n Naming) CodeFilePath(code *Code) string {
	return filesystem.Join(code.Path(), code.CodeFileName)
}

func (n Naming) SharedCodeFilePath(parentPath, targetComponentId string) string {
	return filesystem.Join(parentPath, n.CodeFileName(targetComponentId))
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

func (n Naming) VariablesPath(parentPath string, config *Config) PathInProject {
	if len(parentPath) == 0 {
		panic(fmt.Errorf(`variables "%s" parent path cannot be empty"`, config))
	}

	if config.ComponentId != VariablesComponentId {
		panic(fmt.Errorf(`variables must be from "%s" component, given "%s"`, VariablesComponentId, config.ComponentId))
	}

	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(string(n.Variables), map[string]interface{}{
		"config_id":   config.Id,
		"config_name": utils.NormalizeName(config.Name),
	})
	return n.ensureUniquePath(config.Key(), p)
}

func (n Naming) VariablesValuesPath(parentPath string, row *ConfigRow) PathInProject {
	if len(parentPath) == 0 {
		panic(fmt.Errorf(`variables values "%s" parent path cannot be empty"`, row))
	}

	if row.ComponentId != VariablesComponentId {
		panic(fmt.Errorf(`variables values must be from "%s" component, given "%s"`, VariablesComponentId, row.ComponentId))
	}

	p := PathInProject{}
	p.ParentPath = parentPath
	p.ObjectPath = utils.ReplacePlaceholders(string(n.VariablesValues), map[string]interface{}{
		"config_row_id":   row.Id,
		"config_row_name": utils.NormalizeName(row.Name),
	})
	return n.ensureUniquePath(row.Key(), p)
}
