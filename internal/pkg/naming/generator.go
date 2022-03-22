package naming

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

const (
	MetaFile          = "meta.json"
	ConfigFile        = "config.json"
	DescriptionFile   = "description.md"
	PhaseFile         = "phase.json"
	TaskFile          = "task.json"
	CodeFileName      = `code` // transformation code block name without ext
	blocksDir         = `blocks`
	blockNameTemplate = PathTemplate(`{block_order}-{block_name}`)
	codeNameTemplate  = PathTemplate(`{code_order}-{code_name}`)
	phasesDir         = `phases`
	phaseNameTemplate = PathTemplate(`{phase_order}-{phase_name}`)
	taskNameTemplate  = PathTemplate(`{task_order}-{task_name}`)
)

type Generator struct {
	template   Template
	registry   *Registry
	components *ComponentsMap
}

func NewGenerator(template Template, registry *Registry, components *ComponentsMap) *Generator {
	return &Generator{template: template, registry: registry, components: components}
}

func (g Generator) MetaFilePath(dir string) string {
	return filesystem.Join(dir, MetaFile)
}

func (g Generator) ConfigFilePath(dir string) string {
	return filesystem.Join(dir, ConfigFile)
}

func (g Generator) DescriptionFilePath(dir string) string {
	return filesystem.Join(dir, DescriptionFile)
}

func (g Generator) CodeFilePath(code *Code) string {
	return filesystem.Join(code.String(), code.CodeFileName)
}

func (g Generator) CodeFileName(componentId ComponentId) string {
	return CodeFileName + "." + CodeFileExt(componentId)
}

func (g Generator) SharedCodeFilePath(parentPath string, targetComponentId ComponentId) string {
	return filesystem.Join(parentPath, g.CodeFileName(targetComponentId))
}

func (g Generator) PhaseFilePath(phase *Phase) string {
	return filesystem.Join(phase.String(), PhaseFile)
}

func (g Generator) TaskFilePath(task *Task) string {
	return filesystem.Join(task.String(), TaskFile)
}

func (g Generator) BlocksDir(configDir string) string {
	return filesystem.Join(configDir, blocksDir)
}

func (g Generator) PhasesDir(configDir string) string {
	return filesystem.Join(configDir, phasesDir)
}

func (g Generator) PathFor(object WithKey) (AbsPath, error) {
	switch o := object.(type) {
	case *Branch:
		return g.branchPath(o)
	case *Config:
		return g.configPath(o)
	case *ConfigRow:
		return g.configRowPath(o)
	case *Block:
		return g.blockPath(o)
	case *Code:
		return g.codePath(o)
	case *Phase:
		return g.phasePath(o)
	case *Task:
		return g.taskPath(o)
	default:
		panic(fmt.Errorf(`unexpected type "%T"`, object))
	}
}

func (g Generator) branchPath(branch *Branch) (AbsPath, error) {
	p := AbsPath{}
	p.SetParentPath("") // branch is top level object

	if branch.IsDefault {
		p.SetRelativePath(`main`)
	} else {
		p.SetRelativePath(utils.ReplacePlaceholders(string(g.template.Branch), map[string]interface{}{
			"branch_id":   branch.Id,
			"branch_name": strhelper.NormalizeName(branch.Name),
		}))
	}

	return g.registry.ensureUniquePath(branch.Key(), p), nil
}

func (g Generator) configPath(config *Config) (AbsPath, error) {
	// Get parent
	parentKey, parentKind, parentPath, err := g.getParent(config)
	if err != nil {
		return AbsPath{}, err
	}

	// Check parent
	if !parentKind.IsEmpty() && len(parentPath) == 0 {
		return AbsPath{}, fmt.Errorf(`%s parent path cannot be empty"`, config)
	}

	// Get component
	component, err := g.components.Get(config.ComponentKey())
	if err != nil {
		return AbsPath{}, err
	}

	// Shared code is handled differently
	var template, targetComponentId string
	switch {
	case (parentKind.IsEmpty() || parentKind.IsBranch()) && component.IsSharedCode():
		if config.SharedCode == nil {
			panic(fmt.Errorf(`invalid shared code %s, value is not set`, config.String()))
		}
		// Shared code
		template = string(g.template.SharedCodeConfig)
		targetComponentId = config.SharedCode.Target.String()
	case parentKind.IsConfig() && component.IsScheduler():
		template = string(g.template.SchedulerConfig)
	case parentKind.IsConfig() && component.IsVariables():
		// Regular component with variables
		template = string(g.template.VariablesConfig)
	case parentKind.IsConfigRow() && component.IsVariables() && parentKey.(ConfigRowKey).ComponentId == SharedCodeComponentId:
		// Shared code is config row and can have variables
		template = string(g.template.VariablesConfig)
	case parentKind.IsEmpty() || parentKind.IsBranch():
		// Ordinary config
		template = string(g.template.Config)
	default:
		panic(fmt.Errorf(`unexpected config parent type "%s"`, parentKey.Kind()))
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(template, map[string]interface{}{
		"target_component_id": targetComponentId, // for shared code
		"component_type":      component.Type,
		"component_id":        component.Id,
		"config_id":           jsonnet.StripIdPlaceholder(config.Id.String()),
		"config_name":         strhelper.NormalizeName(config.Name),
	}))
	return g.registry.ensureUniquePath(config.Key(), p), nil
}

func (g Generator) configRowPath(row *ConfigRow) (AbsPath, error) {
	// Get parent
	parentKey, _, parentPath, err := g.getParent(row)
	if err != nil {
		return AbsPath{}, err
	}

	// Check parent
	if !parentKey.Kind().IsConfig() {
		return AbsPath{}, fmt.Errorf(`unexpected config row parent type "%s"`, parentKey.Kind())
	} else if len(parentPath) == 0 {
		return AbsPath{}, fmt.Errorf(`%s parent path cannot be empty"`, row)
	}

	// Get component
	component, err := g.components.Get(row.ComponentKey())
	if err != nil {
		return AbsPath{}, err
	}

	// Shared code is handled differently
	var template string
	switch {
	case component.IsSharedCode():
		template = string(g.template.SharedCodeConfigRow)
	case component.IsVariables():
		template = string(g.template.VariablesValuesRow)
		if row.Relations.Has(VariablesValuesForRelType) {
			template = utils.ReplacePlaceholders(string(g.template.VariablesValuesRow), map[string]interface{}{
				"config_row_name": `default`,
			})
		}
	default:
		template = string(g.template.ConfigRow)
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

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(template, map[string]interface{}{
		"config_row_id":   jsonnet.StripIdPlaceholder(row.Id.String()),
		"config_row_name": strhelper.NormalizeName(name),
	}))
	return g.registry.ensureUniquePath(row.Key(), p), nil
}

func (g Generator) blockPath(block *Block) (AbsPath, error) {
	// Get parent
	_, _, parentPath, err := g.getParent(block)
	if err != nil {
		return AbsPath{}, err
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, block.Index+1),
		"block_name":  strhelper.NormalizeName(block.Name),
	}))
	return g.registry.ensureUniquePath(block.Key(), p), nil
}

func (g Generator) codePath(code *Code) (AbsPath, error) {
	// Get parent
	_, _, parentPath, err := g.getParent(code)
	if err != nil {
		return AbsPath{}, err
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, code.Index+1),
		"code_name":  strhelper.NormalizeName(code.Name),
	}))
	return g.registry.ensureUniquePath(code.Key(), p), nil
}

func (g Generator) phasePath(phase *Phase) (AbsPath, error) {
	_, _, parentPath, err := g.getParent(phase)
	if err != nil {
		return AbsPath{}, err
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(string(phaseNameTemplate), map[string]interface{}{
		"phase_order": fmt.Sprintf(`%03d`, phase.Index+1),
		"phase_name":  strhelper.NormalizeName(phase.Name),
	}))
	return g.registry.ensureUniquePath(phase.Key(), p), nil
}

func (g Generator) taskPath(task *Task) (AbsPath, error) {
	// Get parent
	_, _, parentPath, err := g.getParent(task)
	if err != nil {
		return AbsPath{}, err
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(utils.ReplacePlaceholders(string(taskNameTemplate), map[string]interface{}{
		"task_order": fmt.Sprintf(`%03d`, task.Index+1),
		"task_name":  strhelper.NormalizeName(task.Name),
	}))
	return g.registry.ensureUniquePath(task.Key(), p), nil
}

func (g Generator) getParent(object WithParentKey) (parentKey Key, parentKind Kind, parentPath string, err error) {
	if parentKey, err = object.ParentKey(); err != nil {
		// nop, return err
	} else if parentKey == nil {
		// nop, return empty
	} else if path, found := g.registry.PathByKey(parentKey); found {
		parentKind = parentKey.Kind()
		parentPath = path.String()
	} else {
		err = fmt.Errorf("path generator: %s not found", parentKey.String())
	}
	return parentKey, parentKind, parentPath, err
}
