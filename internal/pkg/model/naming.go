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
	SchedulerConfig     utils.PathTemplate `json:"schedulerConfig" validate:"required"`
	SharedCodeConfig    utils.PathTemplate `json:"sharedCodeConfig" validate:"required"`
	SharedCodeConfigRow utils.PathTemplate `json:"sharedCodeConfigRow" validate:"required"`
	VariablesConfig     utils.PathTemplate `json:"variablesConfig" validate:"required"`
	VariablesValuesRow  utils.PathTemplate `json:"variablesValuesRow" validate:"required"`
	usedLock            *sync.Mutex
	usedByPath          map[string]string        // path -> object key
	usedByKey           map[string]PathInProject // object key -> path
}

func DefaultNaming() *Naming {
	return &Naming{
		Branch:              "{branch_id}-{branch_name}",
		Config:              "{component_type}/{component_id}/{config_id}-{config_name}",
		ConfigRow:           "rows/{config_row_id}-{config_row_name}",
		SchedulerConfig:     "schedules/{config_id}-{config_name}",
		SharedCodeConfig:    "_shared/{target_component_id}",
		SharedCodeConfigRow: "codes/{config_row_id}-{config_row_name}",
		VariablesConfig:     "variables",
		VariablesValuesRow:  "values/{config_row_name}",
		usedLock:            &sync.Mutex{},
		usedByPath:          make(map[string]string),
		usedByKey:           make(map[string]PathInProject),
	}
}

// Attach object's path to Naming, it guarantees the path will remain unique and will not be used again.
func (n Naming) Attach(key Key, path PathInProject) {
	n.usedLock.Lock()
	defer n.usedLock.Unlock()

	// Object path cannot be empty
	pathStr := path.Path()
	if len(pathStr) == 0 {
		panic(fmt.Errorf(`naming error: path for %s cannot be empty`, key.Desc()))
	}

	// Check if the path is unique
	keyStr := key.String()
	if foundKey, found := n.usedByPath[pathStr]; found && foundKey != keyStr {
		panic(fmt.Errorf(
			`naming error: path "%s" is attached to object "%s", but new object "%s" has same path`,
			pathStr, foundKey, keyStr,
		))
	}

	// Remove the previous value attached to the key
	if foundPath, found := n.usedByKey[keyStr]; found {
		delete(n.usedByPath, foundPath.Path())
	}

	n.usedByPath[pathStr] = keyStr
	n.usedByKey[keyStr] = path
}

// Detach object's path from Naming, so it can be used by other object.
func (n Naming) Detach(key Key) {
	n.usedLock.Lock()
	defer n.usedLock.Unlock()

	if foundPath, found := n.usedByKey[key.String()]; found {
		delete(n.usedByPath, foundPath.Path())
		delete(n.usedByKey, key.String())
	}
}

func (n Naming) GetCurrentPath(key Key) (PathInProject, bool) {
	path, found := n.usedByKey[key.String()]
	return path, found
}

func (n Naming) ensureUniquePath(key Key, p PathInProject) PathInProject {
	p = n.makeUniquePath(key, p)
	n.Attach(key, p)
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
	p.SetParentPath("") // branch is top level object

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

	// Get parent in the local filesystem
	parentKey, err := config.ParentKey()
	if err != nil {
		panic(err)
	}
	parent := parentKey.Kind()

	// Shared code is handled differently
	var template, targetComponentId string
	switch {
	case parent.IsBranch() && component.IsSharedCode():
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
	case parent.IsConfig() && component.IsScheduler():
		template = string(n.SchedulerConfig)
	case parent.IsConfig() && component.IsVariables():
		// Regular component with variables
		template = string(n.VariablesConfig)
	case parent.IsConfigRow() && component.IsVariables() && parentKey.(ConfigRowKey).ComponentId == SharedCodeComponentId:
		// Shared code is config row and can have variables
		template = string(n.VariablesConfig)
	case parent.IsBranch():
		// Ordinary config
		template = string(n.Config)
	default:
		panic(fmt.Errorf(`unexpected config parent type "%s"`, parentKey.Kind()))
	}

	p := PathInProject{}
	p.SetParentPath(parentPath)
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

	// Get parent in the local filesystem
	parentKey, err := row.ParentKey()
	if err != nil {
		panic(err)
	}

	// Check parent type
	if !parentKey.Kind().IsConfig() {
		panic(fmt.Errorf(`unexpected config row parent type "%s"`, parentKey.Kind()))
	}

	// Shared code is handled differently
	var template string
	switch {
	case component.IsSharedCode():
		template = string(n.SharedCodeConfigRow)
	case component.IsVariables():
		template = string(n.VariablesValuesRow)
		if row.Relations.Has(VariablesValuesForRelType) {
			template = utils.ReplacePlaceholders(string(n.VariablesValuesRow), map[string]interface{}{
				"config_row_name": `default`,
			})
		}
	default:
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
	p.SetParentPath(parentPath)
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
	p.SetParentPath(parentPath)
	p.ObjectPath = utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, block.Index+1),
		"block_name":  utils.NormalizeName(block.Name),
	})
	return n.ensureUniquePath(block.Key(), p)
}

func (n Naming) CodePath(parentPath string, code *Code) PathInProject {
	p := PathInProject{}
	p.SetParentPath(parentPath)
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

func (n Naming) MatchConfigPath(parentKey Key, path PathInProject) (componentId string, err error) {
	parent := parentKey.Kind()
	if parent.IsBranch() {
		// Shared code
		if matched, _ := n.SharedCodeConfig.MatchPath(path.ObjectPath); matched {
			return SharedCodeComponentId, nil
		}

		// Ordinary config
		if matched, matches := n.Config.MatchPath(path.ObjectPath); matched {
			// Get component ID
			componentId, ok := matches["component_id"]
			if !ok || componentId == "" {
				return "", fmt.Errorf(`config's component id cannot be determined, path: "%s", path template: "%s"`, path.Path(), n.Config)
			}
			return componentId, nil
		}
	}

	// Config embedded in another config
	if parent.IsConfig() {
		// Variables
		if matched, _ := n.VariablesConfig.MatchPath(path.ObjectPath); matched {
			return VariablesComponentId, nil
		}
		// Scheduler
		if matched, _ := n.SchedulerConfig.MatchPath(path.ObjectPath); matched {
			return SchedulerComponentId, nil
		}
	}

	// Shared code variables, parent is config row
	if parent.IsConfigRow() && parentKey.(ConfigRowKey).ComponentId == SharedCodeComponentId {
		if matched, _ := n.VariablesConfig.MatchPath(path.ObjectPath); matched {
			return VariablesComponentId, nil
		}
	}

	return "", nil
}

func (n Naming) MatchConfigRowPath(component *Component, path PathInProject) bool {
	// Shared code
	if component.IsSharedCode() {
		matched, _ := n.SharedCodeConfigRow.MatchPath(path.ObjectPath)
		return matched
	}

	// Variables
	if component.IsVariables() {
		matched, _ := n.VariablesValuesRow.MatchPath(path.ObjectPath)
		return matched
	}

	// Ordinary config row
	matched, _ := n.ConfigRow.MatchPath(path.ObjectPath)
	return matched
}
