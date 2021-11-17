package orchestrator

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func (m *orchestratorMapper) OnObjectsLoad(event model.OnObjectsLoadEvent) error {
	// Only local load
	if event.StateType != model.StateTypeLocal {
		return nil
	}

	errors := utils.NewMultiError()
	for _, object := range event.NewObjects {
		// Object must be orchestrator config
		if ok, err := m.isOrchestratorConfig(object); err != nil || !ok {
			return err
		}
		if err := m.loadLocalPhases(object.(*model.Config)); err != nil {
			manifest := m.State.MustGet(object.Key())
			errors.Append(utils.PrefixError(fmt.Sprintf(`invalid orchestrator config "%s"`, manifest.Path()), err))
		}
	}
	return errors.ErrorOrNil()
}

func (m *orchestratorMapper) loadLocalPhases(config *model.Config) error {
	manifest := m.State.MustGet(config.ConfigKey).Manifest().(*model.ConfigManifest)
	loader := &localLoader{
		MapperContext: m.MapperContext,
		phasesSorter:  newPhasesSorter(),
		branch:        m.State.MustGet(config.BranchKey()).(*model.BranchState),
		config:        config,
		manifest:      manifest,
		phasesDir:     m.Naming.PhasesDir(manifest.Path()),
		errors:        utils.NewMultiError(),
	}
	return loader.load()
}

type localLoader struct {
	model.MapperContext
	*phasesSorter
	branch    *model.BranchState
	config    *model.Config
	manifest  *model.ConfigManifest
	phasesDir string
	errors    *utils.Error
}

func (l *localLoader) load() error {
	// Load phases and tasks from filesystem
	for phaseIndex, phaseDir := range l.phasesDirs() {
		errors := utils.NewMultiError()

		// Process phase
		phase, dependsOn, err := l.addPhase(phaseIndex, phaseDir)
		if err == nil {
			key := phase.ObjectPath
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
					phase.Tasks = append(phase.Tasks, *task)
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
	l.config.Orchestration = &model.Orchestration{}
	for _, phase := range sortedPhases {
		l.config.Orchestration.Phases = append(l.config.Orchestration.Phases, *phase)
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
		PathInProject: model.NewPathInProject(
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
		TaskKey:       model.TaskKey{Index: taskIndex},
		PathInProject: model.NewPathInProject(phase.Path(), path),
	}

	// Track task path
	l.manifest.AddRelatedPath(task.Path())

	// Parse config file
	return task, l.parseTaskConfig(task)
}

func (l *localLoader) parsePhaseConfig(phase *model.Phase) ([]string, error) {
	// Load phase config
	file, err := l.loadJsonFile(l.Naming.PhaseFilePath(*phase), `phase config`)
	if err != nil {
		return nil, err
	}

	errors := utils.NewMultiError()
	phaseContent := file.Content
	parser := &phaseParser{content: phaseContent}

	// Get name
	phase.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get dependsOn
	dependsOn, err := parser.dependsOnPaths()
	if err != nil {
		errors.Append(err)
	}

	// Set additional content
	phase.Content = parser.additionalContent()
	return dependsOn, errors.ErrorOrNil()
}

func (l *localLoader) parseTaskConfig(task *model.Task) error {
	// Load task config
	file, err := l.loadJsonFile(l.Naming.TaskFilePath(*task), `task config`)
	if err != nil {
		return err
	}

	errors := utils.NewMultiError()
	taskContent := file.Content
	parser := &taskParser{content: taskContent}

	// Get name
	task.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get target config path
	configPath, err := parser.configPath()
	if err != nil {
		errors.Append(err)
	}

	// Get target config
	if len(configPath) > 0 {
		configPath = filesystem.Join(l.branch.Path(), configPath)
		configKeyRaw, found := l.Naming.FindByPath(configPath)
		if found {
			if configKey, ok := configKeyRaw.(model.ConfigKey); ok {
				task.ComponentId = configKey.ComponentId
				task.ConfigId = configKey.Id
			} else {
				errors.Append(fmt.Errorf(`path "%s" must be config, found %s`, configPath, configKeyRaw.Kind().String()))
			}
		} else {
			errors.Append(fmt.Errorf(`config "%s" not found`, configPath))
		}
	}

	// Add task to phase
	task.Content = parser.additionalContent()
	return errors.ErrorOrNil()
}

func (l *localLoader) loadJsonFile(path, desc string) (*filesystem.JsonFile, error) {
	if file, err := l.Fs.ReadJsonFile(path, desc); err != nil {
		// Remove absolute path from error
		return nil, fmt.Errorf(strings.ReplaceAll(err.Error(), l.manifest.Path()+string(filesystem.PathSeparator), ``))
	} else {
		l.manifest.AddRelatedPath(path)
		return file, nil
	}
}

func (l *localLoader) phasesDirs() []string {
	// Check if blocks dir exists
	if !l.Fs.IsDir(l.phasesDir) {
		l.errors.Append(fmt.Errorf(`missing phases dir "%s"`, l.phasesDir))
		return nil
	}

	// Track phases dir
	l.manifest.AddRelatedPath(l.phasesDir)

	// Load all dir entries
	dirs, err := filesystem.ReadSubDirs(l.Fs, l.phasesDir)
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read orchestrator phases from "%s": %w`, l.phasesDir, err))
		return nil
	}
	return dirs
}

func (l *localLoader) tasksDirs(phase *model.Phase) []string {
	dirs, err := filesystem.ReadSubDirs(l.Fs, phase.Path())
	if err != nil {
		l.errors.Append(fmt.Errorf(`cannot read orchestrator tasks from "%s": %w`, phase.Path(), err))
		return nil
	}
	return dirs
}
