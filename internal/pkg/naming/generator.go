package naming

import (
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	template Template
	registry *Registry
}

func NewGenerator(template Template, registry *Registry) *Generator {
	return &Generator{template: template, registry: registry}
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

func (g Generator) BranchPath(branch *Branch) AbsPath {
	p := AbsPath{}
	p.SetParentPath("") // branch is top level object

	if branch.IsDefault {
		p.RelativePath = `main`
	} else {
		p.SetRelativePath(strhelper.ReplacePlaceholders(string(g.template.Branch), map[string]interface{}{
			"branch_id":   branch.Id,
			"branch_name": strhelper.NormalizeName(branch.Name),
		}))
	}

	return g.registry.ensureUniquePath(branch.Key(), p)
}

func (g Generator) ConfigPath(parentPath string, component *storageapi.Component, config *Config) AbsPath {
	// Get parent in the local filesystem
	parentKey, err := config.ParentKey()
	if err != nil {
		panic(err)
	}
	var parentKind Kind
	if parentKey != nil {
		parentKind = parentKey.Kind()
	}

	if !parentKind.IsEmpty() && len(parentPath) == 0 {
		panic(errors.Errorf(`config "%s" parent path cannot be empty"`, config))
	}

	// Shared code is handled differently
	var template, targetComponentId string
	switch {
	case (parentKind.IsEmpty() || parentKind.IsBranch()) && component.IsSharedCode():
		if config.SharedCode == nil {
			panic(errors.Errorf(`invalid shared code %s, value is not set`, config.Desc()))
		}
		// Shared code
		template = string(g.template.SharedCodeConfig)
		targetComponentId = config.SharedCode.Target.String()
	case parentKind.IsConfig() && component.IsScheduler():
		template = string(g.template.SchedulerConfig)
	case parentKind.IsConfig() && component.IsVariables():
		// Regular component with variables
		template = string(g.template.VariablesConfig)
	case parentKind.IsConfigRow() && component.IsVariables() && parentKey.(ConfigRowKey).ComponentId == storageapi.SharedCodeComponentID:
		// Shared code is config row and can have variables
		template = string(g.template.VariablesConfig)
	case parentKind.IsEmpty() || parentKind.IsBranch():
		// Ordinary config
		template = string(g.template.Config)
	default:
		panic(errors.Errorf(`unexpected config parent type "%s"`, parentKey.Kind()))
	}

	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(strhelper.ReplacePlaceholders(template, map[string]interface{}{
		"target_component_id": targetComponentId, // for shared code
		"component_type":      component.Type,
		"component_id":        component.ID,
		"config_id":           jsonnet.StripIdPlaceholder(config.Id.String()),
		"config_name":         strhelper.NormalizeName(config.Name),
	}))
	return g.registry.ensureUniquePath(config.Key(), p)
}

func (g Generator) ConfigRowPath(parentPath string, component *storageapi.Component, row *ConfigRow) AbsPath {
	if len(parentPath) == 0 {
		panic(errors.Errorf(`config row "%s" parent path cannot be empty"`, row))
	}

	// Get parent in the local filesystem
	parentKey, err := row.ParentKey()
	if err != nil {
		panic(err)
	}

	// Check parent type
	if !parentKey.Kind().IsConfig() {
		panic(errors.Errorf(`unexpected config row parent type "%s"`, parentKey.Kind()))
	}

	// Shared code is handled differently
	var template string
	switch {
	case component.IsSharedCode():
		template = string(g.template.SharedCodeConfigRow)
	case component.IsVariables():
		template = string(g.template.VariablesValuesRow)
		if row.Relations.Has(VariablesValuesForRelType) {
			template = strhelper.ReplacePlaceholders(string(g.template.VariablesValuesRow), map[string]interface{}{
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
	p.SetRelativePath(strhelper.ReplacePlaceholders(template, map[string]interface{}{
		"config_row_id":   jsonnet.StripIdPlaceholder(row.Id.String()),
		"config_row_name": strhelper.NormalizeName(name),
	}))
	return g.registry.ensureUniquePath(row.Key(), p)
}

func (g Generator) BlocksDir(configDir string) string {
	return filesystem.Join(configDir, blocksDir)
}

func (g Generator) BlockPath(parentPath string, block *Block) AbsPath {
	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(strhelper.ReplacePlaceholders(string(blockNameTemplate), map[string]interface{}{
		"block_order": fmt.Sprintf(`%03d`, block.Index+1),
		"block_name":  strhelper.NormalizeName(block.Name),
	}))
	return g.registry.ensureUniquePath(block.Key(), p)
}

func (g Generator) CodePath(parentPath string, code *Code) AbsPath {
	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(strhelper.ReplacePlaceholders(string(codeNameTemplate), map[string]interface{}{
		"code_order": fmt.Sprintf(`%03d`, code.Index+1),
		"code_name":  strhelper.NormalizeName(code.Name),
	}))
	return g.registry.ensureUniquePath(code.Key(), p)
}

func (g Generator) CodeFilePath(code *Code) string {
	return filesystem.Join(code.Path(), code.CodeFileName)
}

func (g Generator) SharedCodeFilePath(parentPath string, targetComponentId storageapi.ComponentID) string {
	return filesystem.Join(parentPath, g.CodeFileName(targetComponentId))
}

func (g Generator) CodeFileName(componentId storageapi.ComponentID) string {
	return CodeFileName + "." + CodeFileExt(componentId)
}

func (g Generator) PhasesDir(configDir string) string {
	return filesystem.Join(configDir, phasesDir)
}

func (g Generator) PhasePath(parentPath string, phase *Phase) AbsPath {
	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(strhelper.ReplacePlaceholders(string(phaseNameTemplate), map[string]interface{}{
		"phase_order": fmt.Sprintf(`%03d`, phase.Index+1),
		"phase_name":  strhelper.NormalizeName(phase.Name),
	}))
	return g.registry.ensureUniquePath(phase.Key(), p)
}

func (g Generator) PhaseFilePath(phase *Phase) string {
	return filesystem.Join(phase.Path(), PhaseFile)
}

func (g Generator) TaskPath(parentPath string, task *Task) AbsPath {
	p := AbsPath{}
	p.SetParentPath(parentPath)
	p.SetRelativePath(strhelper.ReplacePlaceholders(string(taskNameTemplate), map[string]interface{}{
		"task_order": fmt.Sprintf(`%03d`, task.Index+1),
		"task_name":  strhelper.NormalizeName(task.Name),
	}))
	return g.registry.ensureUniquePath(task.Key(), p)
}

func (g Generator) TaskFilePath(task *Task) string {
	return filesystem.Join(task.Path(), TaskFile)
}
