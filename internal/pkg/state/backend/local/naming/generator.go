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
	BlocksDir         = `blocks`
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
	objects    ObjectsReadOnly
}

type parentDetails struct {
	key  Key
	kind Kind
	path AbsPath
}

func NewGenerator(template Template, registry *Registry, components *ComponentsMap, objects ObjectsReadOnly) *Generator {
	return &Generator{template: template, registry: registry, components: components, objects: objects}
}

func (g Generator) MetaFilePath(parentPath AbsPath) string {
	return filesystem.Join(parentPath.String(), MetaFile)
}

func (g Generator) ConfigFilePath(parentPath AbsPath) string {
	return filesystem.Join(parentPath.String(), ConfigFile)
}

func (g Generator) DescriptionFilePath(parentPath AbsPath) string {
	return filesystem.Join(parentPath.String(), DescriptionFile)
}

func (g Generator) CodeFileName(componentId ComponentId) string {
	return CodeFileName + "." + CodeFileExt(componentId)
}

func (g Generator) SharedCodeFilePath(parentPath AbsPath, targetComponentId ComponentId) string {
	return filesystem.Join(parentPath.String(), g.CodeFileName(targetComponentId))
}

func (g Generator) PhaseFilePath(phaseDir AbsPath) string {
	return filesystem.Join(phaseDir.String(), PhaseFile)
}

func (g Generator) TaskFilePath(taskDir AbsPath) string {
	return filesystem.Join(taskDir.String(), TaskFile)
}

func (g Generator) BlocksDir(configDir AbsPath) AbsPath {
	return NewAbsPath(configDir.String(), BlocksDir)
}

func (g Generator) PhasesDir(configDir AbsPath) AbsPath {
	return NewAbsPath(configDir.String(), phasesDir)
}

func (g Generator) GetOrGenerate(object Object) (AbsPath, error) {
	// Get path
	key := object.Key()
	if path, found := g.registry.PathByKey(key); found {
		return path, nil
	}

	// Generate path
	path, err := g.Generate(object)
	if err != nil {
		return AbsPath{}, err
	}

	return path, nil
}

func (g Generator) Generate(object Object) (AbsPath, error) {
	// Generate
	path, err := g.generate(object)
	if err != nil {
		return AbsPath{}, err
	}

	// Attach
	key := object.Key()
	return g.registry.ensureUniquePath(key, path), nil
}

func (g Generator) generate(object Object) (AbsPath, error) {
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
	p := NewEmptyAbsPath()
	p = p.WithParentPath("")

	if branch.IsDefault {
		p = p.WithRelativePath(`main`)
	} else {
		p = p.WithRelativePath(utils.ReplacePlaceholders(string(g.template.Branch), map[string]interface{}{
			"branch_id":   branch.Id,
			"branch_name": strhelper.NormalizeName(branch.Name),
		}))
	}

	return p, nil
}

func (g Generator) configPath(config *Config) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(config)
	if err != nil {
		return AbsPath{}, err
	}

	// Get component
	component, err := g.components.Get(config.ComponentKey())
	if err != nil {
		return AbsPath{}, err
	}

	// Shared code is handled differently
	var template, targetComponentId string
	switch {
	case (parent.kind.IsEmpty() || parent.kind.IsBranch()) && component.IsSharedCode():
		//if config.SharedCode == nil {
		//	panic(fmt.Errorf(`invalid shared code %s, value is not set`, config.String()))
		//}
		//// Shared code
		//template = string(g.template.SharedCodeConfig)
		//targetComponentId = config.SharedCode.Target.String()
	case parent.kind.IsConfig() && component.IsScheduler():
		template = string(g.template.SchedulerConfig)
	case parent.kind.IsConfig() && component.IsVariables():
		// Regular component with variables
		template = string(g.template.VariablesConfig)
	case parent.kind.IsConfigRow() && component.IsVariables() && parent.key.(ConfigRowKey).ComponentId == SharedCodeComponentId:
		// Shared code is config row and can have variables
		template = string(g.template.VariablesConfig)
	case parent.kind.IsEmpty() || parent.kind.IsBranch():
		// Ordinary config
		template = string(g.template.Config)
	default:
		panic(fmt.Errorf(`unexpected config parent type "%s"`, parent.kind))
	}

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(utils.ReplacePlaceholders(template, map[string]interface{}{
		"target_component_id": targetComponentId, // for shared code
		"component_type":      component.Type,
		"component_id":        component.Id,
		"config_id":           jsonnet.StripIdPlaceholder(config.Id.String()),
		"config_name":         strhelper.NormalizeName(config.Name),
	}))

	return p, nil
}

func (g Generator) configRowPath(row *ConfigRow) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(row)
	if err != nil {
		return AbsPath{}, err
	}

	// Check parent
	if !parent.kind.IsConfig() {
		return AbsPath{}, fmt.Errorf(`unexpected config row parent type "%s"`, parent.kind)
	} else if len(parent.path.String()) == 0 {
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

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(utils.ReplacePlaceholders(template, map[string]interface{}{
		"config_row_id":   jsonnet.StripIdPlaceholder(row.Id.String()),
		"config_row_name": strhelper.NormalizeName(name),
	}))
	return p, nil
}

func (g Generator) blockPath(block *Block) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(block)
	if err != nil {
		return AbsPath{}, err
	}

	relativePath := utils.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, block.Index+1),
		"block_name":  strhelper.NormalizeName(block.Name),
	})

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(relativePath)
	p = p.WithRelativePath(filesystem.Join(BlocksDir, relativePath))
	return p, nil
}

func (g Generator) codePath(code *Code) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(code)
	if err != nil {
		return AbsPath{}, err
	}

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(utils.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, code.Index+1),
		"code_name":  strhelper.NormalizeName(code.Name),
	}))
	return p, nil
}

func (g Generator) phasePath(phase *Phase) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(phase)
	if err != nil {
		return AbsPath{}, err
	}

	relativePath := utils.ReplacePlaceholders(string(phaseNameTemplate), map[string]interface{}{
		"phase_order": fmt.Sprintf(`%03d`, phase.Index+1),
		"phase_name":  strhelper.NormalizeName(phase.Name),
	})

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(filesystem.Join(phasesDir, relativePath))
	return p, nil
}

func (g Generator) taskPath(task *Task) (AbsPath, error) {
	// Get parent
	parent, err := g.getParent(task)
	if err != nil {
		return AbsPath{}, err
	}

	p := NewEmptyAbsPath()
	p = p.WithParentPath(parent.path.String())
	p = p.WithRelativePath(utils.ReplacePlaceholders(string(taskNameTemplate), map[string]interface{}{
		"task_order": fmt.Sprintf(`%03d`, task.Index+1),
		"task_name":  strhelper.NormalizeName(task.Name),
	}))
	return p, nil
}

func (g Generator) getParent(object Object) (*parentDetails, error) {
	// Get parent key
	parentKey, err := object.ParentKey()
	if err != nil {
		return nil, err
	}

	// Has parent?
	if parentKey == nil {
		return nil, nil
	}

	// Get parent path
	parentPath, found := g.registry.PathByKey(parentKey)
	if !found {
		// Get parent object
		parent, found := g.objects.Get(parentKey)
		if !found {
			return nil, fmt.Errorf("naming generator: %s not found", parentKey)
		}

		// Generate parent path
		if path, err := g.Generate(parent); err != nil {
			return nil, err
		} else {
			parentPath = path
		}
	}

	return &parentDetails{key: parentKey, kind: parentKey.Kind(), path: parentPath}, nil
}
