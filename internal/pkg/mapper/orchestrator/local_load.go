package orchestrator

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const PreviousPhaseLink = "<previous>"

func (m *orchestratorMapper) onLocalLoad(config *model.Config, manifest *model.ConfigManifest, allObjects model.Objects) error {
	loader := &localLoader{
		State:        m.state,
		phasesSorter: newPhasesSorter(),
		files:        model.NewFilesLoader(m.state.FileLoader()),
		allObjects:   allObjects,
		config:       config,
		manifest:     manifest,
		phasesDir:    m.state.NamingGenerator().PhasesDir(manifest.Path()),
		errors:       utils.NewMultiError(),
	}

	if err := loader.load(); err != nil {
		return utils.PrefixError(fmt.Sprintf(`invalid orchestrator config "%s"`, manifest.Path()), err)
	}
	return nil
}

type localLoader struct {
	*state.State
	*phasesSorter
	files      *model.FilesLoader
	allObjects model.Objects
	config     *model.Config
	manifest   *model.ConfigManifest
	phasesDir  string
	errors     *utils.MultiError
}

func (l *localLoader) load() error {
	// Load phases and tasks from filesystem
	for phaseIndex, phaseDir := range l.phasesDirs() {
		errors := utils.NewMultiError()

		// Process phase
		phase, dependsOn, err := l.addPhase(phaseIndex, phaseDir)
		if err == nil {
			key := phase.GetRelativePath()
			l.phasesKeys = append(l.phasesKeys, key)
			l.phaseByKey[key] = phase
			l.phaseDependsOnKeys[key] = dependsOn
		} else {
			errors.Append(err)
		}

		// Process tasks
		if errors.Len() == 0 {
			for taskIndex, taskDir := range l.tasksDirs(phase) {
				if task, err := l.addTask(taskIndex, phase, taskDir); err == nil {
					phase.Tasks = append(phase.Tasks, task)
				} else {
					errors.Append(utils.PrefixError(fmt.Sprintf(`invalid task "%s"`, taskDir), err))
				}
			}
		}

		if errors.Len() > 0 {
			l.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid phase "%s"`, phaseDir), errors))
		}
	}

	// Sort by dependencies
	sortedPhases, err := l.sortPhases()
	if err != nil {
		l.errors.Append(err)
	}

	// Convert pointers to values
	l.config.Orchestration = &model.Orchestration{
		Phases: sortedPhases,
	}

	// Track loaded files
	for _, file := range l.files.Loaded() {
		l.manifest.AddRelatedPath(file.Path())
	}

	return l.errors.ErrorOrNil()
}

func (l *localLoader) addPhase(phaseIndex int, path string) (*model.Phase, []string, error) {
	// Create struct
	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchId:    l.config.BranchId,
			ComponentId: l.config.ComponentId,
			ConfigId:    l.config.Id,
			Index:       phaseIndex,
		},
		AbsPath: model.NewAbsPath(
			l.phasesDir,
			path,
		),
	}

	// Track phase path
	l.manifest.AddRelatedPath(phase.Path())

	// Parse config file
	dependsOn, err := l.parsePhaseConfig(phase)
	return phase, dependsOn, err
}

func (l *localLoader) addTask(taskIndex int, phase *model.Phase, path string) (*model.Task, error) {
	// Create struct
	task := &model.Task{
		TaskKey: model.TaskKey{Index: taskIndex},
		AbsPath: model.NewAbsPath(phase.Path(), path),
	}

	// Track task path
	l.manifest.AddRelatedPath(task.Path())

	// Parse config file
	return task, l.parseTaskConfig(task)
}

func (l *localLoader) parsePhaseConfig(phase *model.Phase) ([]string, error) {
	// Load phase config
	file, err := l.files.
		Load(l.NamingGenerator().PhaseFilePath(phase)).
		AddMetadata(filesystem.ObjectKeyMetadata, phase.Key()).
		SetDescription("phase config").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindPhaseConfig).
		ReadJsonFile()
	if err != nil {
		return nil, l.formatError(err)
	}

	parser := &phaseParser{content: file.Content}
	errors := utils.NewMultiError()

	// Get name
	phase.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get dependsOn
	dependsOnRaw, err := parser.dependsOnPaths()
	if err != nil {
		errors.Append(err)
	}

	// Process links to previous phase
	var dependsOn []string
	for _, item := range dependsOnRaw {
		if strings.TrimSpace(item) == PreviousPhaseLink {
			// Add previous phase if any exists
			if len(l.phasesKeys) > 0 {
				dependsOn = append(dependsOn, l.phasesKeys[len(l.phasesKeys)-1])
			}
		} else {
			dependsOn = append(dependsOn, item)
		}
	}

	// Set additional content
	phase.Content = parser.additionalContent()
	return dependsOn, errors.ErrorOrNil()
}

func (l *localLoader) parseTaskConfig(task *model.Task) error {
	// Load task config
	file, err := l.files.
		Load(l.NamingGenerator().TaskFilePath(task)).
		AddMetadata(filesystem.ObjectKeyMetadata, task.Key()).
		SetDescription("task config").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindTaskConfig).
		ReadJsonFile()
	if err != nil {
		return l.formatError(err)
	}

	parser := &taskParser{content: file.Content}
	errors := utils.NewMultiError()

	// Get name
	task.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get enabled
	task.Enabled, err = parser.enabled()
	if err != nil {
		errors.Append(err)
	}

	// Load configPath, or configData and componentId
	if parser.hasConfigPath() {
		// Get target config path
		targetConfigPath, err := parser.configPath()
		if err != nil {
			errors.Append(err)
		} else {
			// Get target config
			targetConfig, err := l.getTargetConfig(targetConfigPath)
			if err != nil {
				errors.Append(err)
			} else if targetConfig != nil {
				task.ComponentId = targetConfig.ComponentId
				task.ConfigId = targetConfig.Id
				task.ConfigPath = l.MustGet(targetConfig.Key()).Path()
				markConfigUsedInOrchestrator(targetConfig, l.config)
			}
		}
	} else if parser.hasConfigData() {
		// Get config data
		if task.ConfigData, err = parser.configData(); err != nil {
			errors.Append(err)
		}
		if task.ComponentId, err = parser.componentId(); err != nil {
			errors.Append(err)
		}
	} else {
		if task.Enabled {
			errors.Append(fmt.Errorf("task.configPath, or task.configData and task.componentId must be specified"))
		} else if task.ComponentId, err = parser.componentId(); err != nil {
			// ComponentId is required even when the task is disabled (for UI)
			errors.Append(err)
		}
	}

	// Add task to phase
	task.Content = parser.additionalContent()
	return errors.ErrorOrNil()
}

func (l *localLoader) getTargetConfig(targetPath string) (*model.Config, error) {
	if len(targetPath) == 0 {
		return nil, nil
	}

	targetPath = filesystem.Join(l.manifest.GetParentPath(), targetPath)
	configStateRaw, found := l.GetByPath(targetPath)
	if !found || !configStateRaw.HasLocalState() {
		return nil, fmt.Errorf(`target config "%s" not found`, targetPath)
	}

	configState, ok := configStateRaw.(*model.ConfigState)
	if !ok {
		return nil, fmt.Errorf(`path "%s" must be config, found "%s"`, targetPath, configStateRaw.Kind().String())
	}

	return configState.Local, nil
}

func (l *localLoader) phasesDirs() []string {
	// Check if blocks dir exists
	if !l.ObjectsRoot().IsDir(l.phasesDir) {
		l.errors.Append(fmt.Errorf(`missing phases dir "%s"`, l.phasesDir))
		return nil
	}

	// Track phases dir
	l.manifest.AddRelatedPath(l.phasesDir)

	// Track .gitkeep, .gitignore
	if path := filesystem.Join(l.phasesDir, `.gitkeep`); l.ObjectsRoot().IsFile(path) {
		l.manifest.AddRelatedPath(path)
	}
	if path := filesystem.Join(l.phasesDir, `.gitignore`); l.ObjectsRoot().IsFile(path) {
		l.manifest.AddRelatedPath(path)
	}

	// Load all dir entries
	dirs, err := l.FileLoader().ReadSubDirs(l.ObjectsRoot(), l.phasesDir)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read orchestrator phases from "%s": %w`, l.phasesDir, err))
		return nil
	}
	return dirs
}

func (l *localLoader) tasksDirs(phase *model.Phase) []string {
	dirs, err := l.FileLoader().ReadSubDirs(l.ObjectsRoot(), phase.Path())
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read orchestrator tasks from "%s": %w`, phase.Path(), err))
		return nil
	}
	return dirs
}

func (l *localLoader) formatError(err error) error {
	// Remove absolute path from error
	return fmt.Errorf(strings.ReplaceAll(err.Error(), l.manifest.Path()+string(filesystem.PathSeparator), ``))
}
