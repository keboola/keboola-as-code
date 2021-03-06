package orchestrator

import (
	"context"
	"fmt"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func (m *orchestratorMapper) MapBeforeLocalSave(ctx context.Context, recipe *model.LocalSaveRecipe) error {
	// Object must be orchestrator config
	if ok, err := m.isOrchestratorConfigKey(recipe.Object.Key()); err != nil || !ok {
		return err
	}

	writer := &localWriter{
		State:           m.state,
		LocalSaveRecipe: recipe,
		logger:          m.logger,
		config:          recipe.Object.(*model.Config),
		configPath:      recipe.ObjectManifest.GetAbsPath(),
	}
	writer.save()
	return nil
}

type localWriter struct {
	*state.State
	*model.LocalSaveRecipe
	logger     log.Logger
	config     *model.Config
	configPath model.AbsPath
}

func (w *localWriter) save() {
	phasesDir := w.NamingGenerator().PhasesDir(w.ObjectManifest.Path())

	// Generate ".gitkeep" to preserve the "phases" directory, even if there are no phases.
	w.Files.
		Add(filesystem.NewRawFile(filesystem.Join(phasesDir, `.gitkeep`), ``)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindGitKeep)

	// Generate files for phases
	errors := utils.NewMultiError()
	allPhases := w.config.Orchestration.Phases
	for _, phase := range allPhases {
		if err := w.savePhase(phase, allPhases); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save phase "%s"`, phase.RelativePath), err))
		}
	}

	// Delete all old files from blocks dir
	// We always do full generation of phases dir.
	for _, path := range w.TrackedPaths() {
		if filesystem.IsFrom(path, phasesDir) && w.IsFile(path) {
			w.ToDelete = append(w.ToDelete, path)
		}
	}

	// Convert errors to warning
	if errors.Len() > 0 {
		w.logger.Warn(utils.PrefixError(fmt.Sprintf(`Warning: cannot save orchestrator config "%s"`, w.ObjectManifest.Path()), errors))
	}
}

func (w *localWriter) savePhase(phase *model.Phase, allPhases []*model.Phase) error {
	// Validate
	if err := validator.Validate(w.State.Ctx(), phase); err != nil {
		return err
	}

	// Create content
	errors := utils.NewMultiError()
	phaseContent := orderedmap.New()
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
	w.Files.
		Add(filesystem.NewJsonFile(filesystem.Join(w.NamingGenerator().PhaseFilePath(phase)), phaseContent)).
		SetDescription(`phase config file`).
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindPhaseConfig)

	// Write tasks
	for _, task := range phase.Tasks {
		if err := w.saveTask(task); err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot save task "%s"`, task.RelativePath), err))
		}
	}

	return errors.ErrorOrNil()
}

func (w *localWriter) saveTask(task *model.Task) error {
	// Create content
	errors := utils.NewMultiError()
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

	// Target key
	targetKey := &model.ConfigKey{
		BranchId:    task.BranchId,
		ComponentId: task.ComponentId,
		Id:          task.ConfigId,
	}

	// Get target config
	targetConfig, found := w.Get(targetKey)
	if found {
		// Get target path
		targetPath, err := filesystem.Rel(w.configPath.GetParentPath(), targetConfig.Path())
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
	file := filesystem.
		NewJsonFile(filesystem.Join(w.NamingGenerator().TaskFilePath(task)), taskContent).
		SetDescription(`task config file`)
	w.Files.
		Add(file).
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindTaskConfig)

	return errors.ErrorOrNil()
}
