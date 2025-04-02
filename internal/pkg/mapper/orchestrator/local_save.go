package orchestrator

import (
	"context"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
		configPath:      recipe.GetAbsPath(),
	}
	writer.save(ctx)
	return nil
}

type localWriter struct {
	*state.State
	*model.LocalSaveRecipe
	logger     log.Logger
	config     *model.Config
	configPath model.AbsPath
}

func (w *localWriter) save(ctx context.Context) {
	phasesDir := w.NamingGenerator().PhasesDir(w.Path())

	// Generate ".gitkeep" to preserve the "phases" directory, even if there are no phases.
	w.Files.
		Add(filesystem.NewRawFile(filesystem.Join(phasesDir, `.gitkeep`), ``)).
		AddTag(model.FileTypeOther).
		AddTag(model.FileKindGitKeep)

	// Generate files for phases
	errs := errors.NewMultiError()
	allPhases := w.config.Orchestration.Phases
	for _, phase := range allPhases {
		if err := w.savePhase(phase, allPhases); err != nil {
			errs.AppendWithPrefixf(err, `cannot save phase "%s"`, phase.RelativePath)
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
	if errs.Len() > 0 {
		err := errors.PrefixErrorf(errs, `cannot save orchestrator config "%s"`, w.Path())
		w.logger.Warn(ctx, errors.Format(errors.PrefixError(err, "warning"), errors.FormatAsSentences()))
	}
}

func (w *localWriter) savePhase(phase *model.Phase, allPhases []*model.Phase) error {
	// Validate
	if err := w.ValidateValue(phase); err != nil {
		return err
	}

	// Create content
	errs := errors.NewMultiError()
	phaseContent := orderedmap.New()
	phaseContent.Set(`name`, phase.Name)

	// Map dependsOn key -> path
	dependsOn := make([]string, 0)
	for _, depOnKey := range phase.DependsOn {
		depOnPhase := allPhases[depOnKey.Index]
		depOnPath, err := filesystem.Rel(phase.GetParentPath(), depOnPhase.Path())
		if err != nil {
			errs.Append(err)
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
		Add(filesystem.NewJSONFile(filesystem.Join(w.NamingGenerator().PhaseFilePath(phase)), phaseContent)).
		SetDescription(`phase config file`).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindPhaseConfig)

	// Write tasks
	for _, task := range phase.Tasks {
		if err := w.saveTask(task); err != nil {
			errs.AppendWithPrefixf(err, `cannot save task "%s"`, task.RelativePath)
		}
	}

	return errs.ErrorOrNil()
}

func (w *localWriter) saveTask(task *model.Task) error {
	// Create content
	errs := errors.NewMultiError()
	taskContent := orderedmap.New()
	taskContent.Set(`name`, task.Name)
	taskContent.Set(`enabled`, task.Enabled)

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

	// Set configId
	switch {
	case len(task.ConfigID) > 0:
		// Target key
		targetKey := &model.ConfigKey{
			BranchID:    task.BranchID,
			ComponentID: task.ComponentID,
			ID:          task.ConfigID,
		}

		// Get target config
		targetConfig, found := w.Get(targetKey)
		if found {
			// Get target path
			targetPath, err := filesystem.Rel(w.configPath.GetParentPath(), targetConfig.Path())
			if err != nil {
				errs.Append(err)
			}

			// Set config path
			target.Set(`configPath`, targetPath)
			taskContent.Set(`task`, *target)
		} else {
			errs.Append(errors.Errorf(`%s not found`, targetKey.Desc()))
		}
	case task.ConfigData != nil:
		target.Set("configData", task.ConfigData)
		target.Set(`componentId`, task.ComponentID)
	default:
		if task.Enabled {
			errs.Append(errors.New("task.configId, or task.configData and task.componentId must be specified"))
		} else {
			// ComponentID is required even when the task is disabled (for UI)
			target.Set(`componentId`, task.ComponentID)
		}
	}

	// Create file
	file := filesystem.
		NewJSONFile(filesystem.Join(w.NamingGenerator().TaskFilePath(task)), taskContent).
		SetDescription(`task config file`)
	w.Files.
		Add(file).
		AddTag(model.FileTypeJSON).
		AddTag(model.FileKindTaskConfig)

	return errs.ErrorOrNil()
}
