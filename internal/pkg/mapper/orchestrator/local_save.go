package orchestrator

import (
	"fmt"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func (m *orchestratorMapper) MapBeforeLocalSave(recipe *model.LocalSaveRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestratorConfigKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}

	writer := &localWriter{
		MapperContext:   m.MapperContext,
		LocalSaveRecipe: recipe,
		config:          recipe.Object.(*model.Config),
	}
	writer.save()
	return nil
}

type localWriter struct {
	model.MapperContext
	*model.LocalSaveRecipe
	config *model.Config
}

func (w *localWriter) save() {
	phasesDir := w.Naming.PhasesDir(w.ObjectManifest.Path())

	// Generate ".gitkeep" to preserve the "phases" directory, even if there are no phases.
	gitKeep := filesystem.NewFile(filesystem.Join(phasesDir, `.gitkeep`), ``)
	w.ExtraFiles = append(w.ExtraFiles, gitKeep)

	// Generate files for phases
	errors := utils.NewMultiError()
	allPhases := w.config.Orchestration.Phases
	for _, phase := range allPhases {
		if err := w.savePhase(phase, allPhases); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save phase "%s"`, phase.ObjectPath), err))
		}
	}

	// Delete all old files from blocks dir
	// We always do full generation of phases dir.
	for _, path := range w.State.TrackedPaths() {
		if filesystem.IsFrom(path, phasesDir) && w.State.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	// Convert errors to warning
	if errors.Len() > 0 {
		w.Logger.Warn(utils.PrefixError(fmt.Sprintf(`Warning: cannot save orchestrator config "%s"`, w.ObjectManifest.Path()), errors))
	}
}

func (w *localWriter) savePhase(phase *model.Phase, allPhases []*model.Phase) error {
	// Validate
	if err := validator.Validate(phase); err != nil {
		return err
	}

	// Create content
	errors := utils.NewMultiError()
	phaseContent := utils.NewOrderedMap()
	phaseContent.Set(`name`, phase.Name)

	// Map dependsOn key -> path
	dependsOn := make([]string, 0)
	for _, depOnKey := range phase.DependsOn {
		depOnPhase := allPhases[depOnKey.Index]
		depOnPath, err := filesystem.Rel(phase.GetParentPath(), depOnPhase.Path())
		if err != nil {
			errors.Append(err)
			continue
		}
		dependsOn = append(dependsOn, depOnPath)
	}
	phaseContent.Set(`dependsOn`, dependsOn)

	// Copy content
	for _, k := range phase.Content.Keys() {
		v, _ := phase.Content.Get(k)
		phaseContent.Set(k, v)
	}

	// Create file
	file, err := filesystem.
		NewJsonFile(filesystem.Join(w.Naming.PhaseFilePath(phase)), phaseContent).
		SetDescription(`phase config file`).
		ToFile()
	if err == nil {
		w.ExtraFiles = append(w.ExtraFiles, file)
	} else {
		errors.Append(err)
	}

	// Write tasks
	for _, task := range phase.Tasks {
		if err := w.saveTask(task); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save task "%s"`, task.ObjectPath), err))
		}
	}

	return errors.ErrorOrNil()
}

func (w *localWriter) saveTask(task *model.Task) error {
	// Create content
	errors := utils.NewMultiError()
	taskContent := utils.NewOrderedMap()
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
		if v, ok := taskMapRaw.(orderedmap.OrderedMap); ok {
			target = &v
		}
	}
	if target == nil {
		target = utils.NewOrderedMap()
	}

	// Get parent branch
	branch := w.State.MustGet(task.ConfigKey().BranchKey())

	// Target key
	targetKey := &model.ConfigKey{
		BranchId:    task.BranchId,
		ComponentId: task.ComponentId,
		Id:          task.ConfigId,
	}

	// Get target config
	targetConfig, found := w.State.Get(targetKey)
	if found {
		// Get target path
		targetPath, err := filesystem.Rel(branch.Path(), targetConfig.Path())
		if err != nil {
			errors.Append(err)
		}

		// Set config path
		target.Set(`configPath`, targetPath)
		taskContent.Set(`task`, *target)
	} else {
		errors.Append(fmt.Errorf(`%s not found`, targetKey.Desc()))
	}

	// Create file
	file, err := filesystem.
		NewJsonFile(filesystem.Join(w.Naming.TaskFilePath(task)), taskContent).
		SetDescription(`task config file`).
		ToFile()
	if err == nil {
		w.ExtraFiles = append(w.ExtraFiles, file)
	} else {
		errors.Append(err)
	}

	return errors.ErrorOrNil()
}
