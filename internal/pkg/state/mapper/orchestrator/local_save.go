package orchestrator

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type localSaveContext struct {
	*model.LocalSaveRecipe
	state        *local.State
	logger       log.Logger
	orchestrator *model.Config
	phases       []*model.Phase
	basePath     model.AbsPath
	phasesDir    model.AbsPath
}

func (m *orchestratorLocalMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestrator(recipe.Object.Key()); err != nil || !ok {
		return err
	}
	orchestrator := recipe.Object.(*model.Config)

	basePath, err := m.state.GetPath(orchestrator.Key())
	if err != nil {
		return err
	}

	saveCtx := &localSaveContext{
		LocalSaveRecipe: recipe,
		state:           m.state,
		logger:          m.logger,
		orchestrator:    orchestrator,
		phases:          orchestrator.Orchestration.Phases,
		basePath:        basePath,
		phasesDir:       m.state.NamingGenerator().PhasesDir(basePath),
	}
	saveCtx.save()
	return nil
}

func (c *localSaveContext) save() {
	// Generate ".gitkeep" to preserve the "phases" directory, even if there are no phases.
	c.Files.
		Add(filesystem.NewRawFile(filesystem.Join(c.phasesDir.String(), `.gitkeep`), ``)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindGitKeep)

	// Generate files for phases
	errors := utils.NewMultiError()
	for _, phase := range c.phases {
		// Get phase path
		phaseDir, err := c.state.GetOrGeneratePath(phase)
		if err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save %s`, phase.String()), err))
			continue
		}

		// Save phase
		if err := c.savePhase(phase, phaseDir); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save phase "%s"`, phaseDir.Base()), err))
		}
	}

	// Delete all old files from phases dir
	// We always do full generation of phases dir.
	for _, path := range c.state.TrackedPaths() {
		if filesystem.IsFrom(path, c.phasesDir.String()) && c.state.ObjectsRoot().IsFile(path) {
			c.ToDelete = append(c.ToDelete, path)
		}
	}

	// Convert errors to warning
	if errors.Len() > 0 {
		c.logger.Warn(utils.PrefixError(fmt.Sprintf(`Warning: cannot save orchestrator config "%s"`, c.basePath), errors))
	}
}

func (c *localSaveContext) savePhase(phase *model.Phase, phaseDir model.AbsPath) error {
	// Validate
	if err := validator.Validate(c.state.Ctx(), phase); err != nil {
		return err
	}

	// Create content
	errors := utils.NewMultiError()
	phaseContent := orderedmap.New()
	phaseContent.Set(`name`, phase.Name)

	// Map dependsOn key -> path
	dependsOn := make([]string, 0)
	for _, depOnKey := range phase.DependsOn {
		depOnPhase := c.phases[depOnKey.Index]
		depOnPath, err := c.state.GetPath(depOnPhase.Key())
		if err != nil {
			errors.Append(err)
			continue
		}
		dependsOn = append(dependsOn, depOnPath.Base())
	}
	phaseContent.Set(`dependsOn`, dependsOn)

	// Copy content
	for _, k := range phase.Content.Keys() {
		v, _ := phase.Content.Get(k)
		phaseContent.Set(k, v)
	}

	// Create file
	c.Files.
		Add(filesystem.NewJsonFile(filesystem.Join(c.state.NamingGenerator().PhaseFilePath(phaseDir)), phaseContent)).
		SetDescription(`phase config file`).
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindPhaseConfig)

	// Write tasks
	for _, task := range phase.Tasks {
		// Get task path
		taskDir, err := c.state.GetOrGeneratePath(task)
		if err != nil {
			errors.Append(err)
			continue
		}

		if err := c.saveTask(task, taskDir); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save task "%s"`, taskDir.Base()), err))
		}
	}

	return errors.ErrorOrNil()
}

func (c *localSaveContext) saveTask(task *model.Task, taskDir model.AbsPath) error {
	// Create content
	taskContent := orderedmap.New()
	taskContent.Set(`name`, task.Name)

	// Copy additional content
	for _, k := range task.Content.Keys() {
		v, _ := task.Content.Get(k)
		taskContent.Set(k, v)
	}

	// Get "task" value
	var target *orderedmap.OrderedMap
	taskMapRaw, found := task.Content.Get(`task`)
	if found {
		if v, ok := taskMapRaw.(*orderedmap.OrderedMap); ok {
			target = v
		}
	}
	if target == nil {
		target = orderedmap.New()
	}

	// Target config
	targetConfigKey := model.ConfigKey{
		BranchId:    task.BranchId,
		ComponentId: task.ComponentId,
		Id:          task.ConfigId,
	}

	// Target path
	targetPath, err := c.getTargetPath(targetConfigKey)
	if err != nil {
		return err
	}

	// Set values
	target.Set(`configPath`, targetPath)
	taskContent.Set(`task`, *target)

	// Create file
	file := filesystem.
		NewJsonFile(filesystem.Join(c.state.NamingGenerator().TaskFilePath(taskDir)), taskContent).
		SetDescription(`task config file`)
	c.Files.
		Add(file).
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindTaskConfig)

	return nil
}

func (c *localSaveContext) getTargetPath(targetConfigKey model.ConfigKey) (string, error) {
	targetConfig, found := c.state.Get(targetConfigKey)
	if !found {
		return "", fmt.Errorf(`%s not found`, targetConfigKey.String())
	}

	absPath, err := c.state.GetPath(targetConfig.Key())
	if err != nil {
		return "", err
	}

	relativePath, err := filesystem.Rel(c.basePath.ParentPath(), absPath.String())
	if err != nil {
		return "", err
	}

	return relativePath, nil
}
