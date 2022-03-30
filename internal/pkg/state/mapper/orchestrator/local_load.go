package orchestrator

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type localLoadContext struct {
	state         *local.State
	phasesSorter  *phasesSorter
	files         *model.FilesLoader
	orchestrator  *model.Config
	basePath      model.AbsPath
	phasesDirsMap map[*model.Phase]model.AbsPath
	tasksDirsMap  map[*model.Task]model.AbsPath
	relatedPaths  *relatedpaths.Paths
	errors        *utils.MultiError
}

func (m *orchestratorLocalMapper) AfterLocalOperation(changes *model.Changes) error {
	errors := utils.NewMultiError()

	// Process all loaded orchestrators
	for _, object := range changes.Loaded() {
		if ok, err := m.isOrchestrator(object.Key()); err != nil {
			errors.Append(err)
		} else if ok {
			if err := m.onLocalLoad(object.(*model.Config)); err != nil {
				errors.Append(err)
			}
		}
	}

	return errors.ErrorOrNil()
}

func (m *orchestratorLocalMapper) onLocalLoad(orchestrator *model.Config) error {
	basePath, err := m.state.GetPath(orchestrator.Key())
	if err != nil {
		return err
	}

	relatedPaths, err := m.state.GetRelatedPaths(orchestrator.Key())
	if err != nil {
		return err
	}

	loadCtx := &localLoadContext{
		state:         m.state,
		phasesSorter:  newPhasesSorter(),
		files:         model.NewFilesLoader(m.state.FileLoader()),
		orchestrator:  orchestrator,
		basePath:      basePath,
		phasesDirsMap: make(map[*model.Phase]model.AbsPath),
		tasksDirsMap:  make(map[*model.Task]model.AbsPath),
		relatedPaths:  relatedPaths,
		errors:        utils.NewMultiError(),
	}

	if err := loadCtx.load(); err != nil {
		return utils.PrefixError(fmt.Sprintf(`invalid orchestrator config "%s"`, basePath), err)
	}
	return nil
}

func (c *localLoadContext) load() error {
	// Load phases from filesystem
	for _, phaseDir := range c.phasesDirs() {
		if err := c.loadPhase(phaseDir); err != nil {
			c.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid phase "%s"`, phaseDir.RelativePath()), err))
		}
	}

	// Sort phases by dependencies
	sortedPhases, err := c.phasesSorter.sortPhases()
	if err != nil {
		c.errors.Append(err)
	}

	// Phase and task keys are now completed, after the sorting.
	// Attach paths to the naming registry.
	for _, phase := range sortedPhases {
		if err := c.state.NamingRegistry().Attach(phase.PhaseKey, c.phasesDirsMap[phase]); err != nil {
			c.errors.Append(err)
		}
		for _, task := range phase.Tasks {
			if err := c.state.NamingRegistry().Attach(task.TaskKey, c.tasksDirsMap[task]); err != nil {
				c.errors.Append(err)
			}
		}
	}

	// Add loaded files to the related paths
	for _, file := range c.files.Loaded() {
		c.relatedPaths.Add(file.Path())
	}

	// Set value
	c.orchestrator.Orchestration = &model.Orchestration{
		Phases: sortedPhases,
	}

	return c.errors.ErrorOrNil()
}

func (c *localLoadContext) loadPhase(phaseDir model.AbsPath) error {
	// Create struct
	phase := &model.Phase{
		PhaseKey: model.PhaseKey{
			BranchId:    c.orchestrator.BranchId,
			ComponentId: c.orchestrator.ComponentId,
			ConfigId:    c.orchestrator.Id,
		},
	}

	// Add phase dir to the related paths
	c.relatedPaths.Add(phaseDir.String())

	// The phase key is not complete yet, the Index is set up after phases sorting.
	// So the phase dir is stored and processed later.
	c.phasesDirsMap[phase] = phaseDir

	// Parse config file
	dependsOn, err := c.loadPhaseConfig(phase, phaseDir)
	if err != nil {
		return err
	}

	// Process tasks
	errors := utils.NewMultiError()
	for taskIndex, taskDir := range c.tasksDirs(phaseDir.String()) {
		if task, err := c.loadTask(taskIndex, taskDir); err == nil {
			phase.Tasks = append(phase.Tasks, task)
		} else {
			errors.Append(utils.PrefixError(fmt.Sprintf(`invalid task "%s"`, taskDir.RelativePath()), err))
		}
	}

	// Add to sorter
	c.phasesSorter.addPhase(phaseDir.RelativePath(), phase, dependsOn)

	return errors.ErrorOrNil()
}

func (c *localLoadContext) loadTask(taskIndex int, taskDir model.AbsPath) (*model.Task, error) {
	// Create struct
	task := &model.Task{
		// Other parts of the key will be filed after phases sort
		TaskKey: model.TaskKey{Index: taskIndex},
	}

	// Add task dir to the related paths
	c.relatedPaths.Add(taskDir.String())

	// The task key is not complete yet, the PhaseKey is set up after phases sorting.
	// So the task dir is stored and processed later.
	c.tasksDirsMap[task] = taskDir

	// Parse config file
	return task, c.loadTaskConfig(task, taskDir)
}

func (c *localLoadContext) getTargetConfig(targetPath string) (*model.Config, error) {
	if len(targetPath) == 0 {
		return nil, nil
	}

	// Get config by path
	targetPath = filesystem.Join(c.basePath.ParentPath(), targetPath)
	configRaw, found := c.state.GetByPath(targetPath)
	if !found {
		return nil, fmt.Errorf(`target config "%s" not found`, targetPath)
	}

	// Check object path
	config, ok := configRaw.(*model.Config)
	if !ok {
		return nil, fmt.Errorf(`path "%s" must be config, found "%s"`, targetPath, configRaw.Kind().String())
	}

	return config, nil
}

func (c *localLoadContext) loadPhaseConfig(phase *model.Phase, phaseDir model.AbsPath) (dependsOn []string, err error) {
	// Load phase config
	file, err := c.files.
		Load(c.state.NamingGenerator().PhaseFilePath(phaseDir)).
		SetDescription("phase config").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindPhaseConfig).
		ReadJsonFile()
	if err != nil {
		return nil, c.formatError(err)
	}

	parser := &phaseParser{content: file.Content}
	errors := utils.NewMultiError()

	// Get name
	phase.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get dependsOn
	dependsOn, err = parser.dependsOnPaths()
	if err != nil {
		errors.Append(err)
	}

	// Set additional content
	phase.Content = parser.additionalContent()
	return dependsOn, errors.ErrorOrNil()
}

func (c *localLoadContext) loadTaskConfig(task *model.Task, taskDir model.AbsPath) error {
	// Load task config
	file, err := c.files.
		Load(c.state.NamingGenerator().TaskFilePath(taskDir)).
		SetDescription("task config").
		AddTag(model.FileTypeJson).
		AddTag(model.FileKindTaskConfig).
		ReadJsonFile()
	if err != nil {
		return c.formatError(err)
	}

	parser := &taskParser{content: file.Content}
	errors := utils.NewMultiError()

	// Get name
	task.Name, err = parser.name()
	if err != nil {
		errors.Append(err)
	}

	// Get target config path
	targetConfigPath, err := parser.configPath()
	if err != nil {
		errors.Append(err)
	}

	// Get target config
	targetConfig, err := c.getTargetConfig(targetConfigPath)
	if err != nil {
		errors.Append(err)
	} else if targetConfig != nil {
		task.ComponentId = targetConfig.ComponentId
		task.ConfigId = targetConfig.Id
		markConfigUsedInOrchestrator(targetConfig, c.orchestrator)
	}

	// Add task to phase
	task.Content = parser.additionalContent()
	return errors.ErrorOrNil()
}

func (c *localLoadContext) phasesDirs() []model.AbsPath {
	fs := c.state.ObjectsRoot()
	phasesDir := c.state.NamingGenerator().PhasesDir(c.basePath)

	// Check if blocks dir exists
	if !fs.IsDir(phasesDir) {
		c.errors.Append(fmt.Errorf(`missing phases dir "%s"`, phasesDir))
		return nil
	}

	// Add phases dir to the related paths
	c.relatedPaths.Add(phasesDir)

	// Add .gitkeep, .gitignore to the related paths
	if path := filesystem.Join(phasesDir, `.gitkeep`); fs.IsFile(path) {
		c.relatedPaths.Add(path)
	}
	if path := filesystem.Join(phasesDir, `.gitignore`); fs.IsFile(path) {
		c.relatedPaths.Add(path)
	}

	// Read all sub-dirs
	phasesDirs, err := filesystem.ReadSubDirs(fs, phasesDir)
	if err != nil {
		c.errors.Append(fmt.Errorf(`cannot read orchestrator phases from "%s": %w`, phasesDir, err))
		return nil
	}

	// Convert to []AbsPath
	out := make([]model.AbsPath, len(phasesDirs))
	for i, dir := range phasesDirs {
		out[i] = model.NewAbsPath(phasesDir, dir)

	}
	return out
}

func (c *localLoadContext) tasksDirs(phaseDir string) []model.AbsPath {
	fs := c.state.ObjectsRoot()

	// Read all sub-dirs
	tasksDirs, err := filesystem.ReadSubDirs(fs, phaseDir)
	if err != nil {
		c.errors.Append(fmt.Errorf(`cannot read orchestrator tasks from "%s": %w`, phaseDir, err))
		return nil
	}

	// Convert to []AbsPath
	out := make([]model.AbsPath, len(tasksDirs))
	for i, dir := range tasksDirs {
		out[i] = model.NewAbsPath(phaseDir, dir)
	}
	return out
}

func (c *localLoadContext) formatError(err error) error {
	// Remove absolute path from the error
	return fmt.Errorf(strings.ReplaceAll(err.Error(), c.basePath.String()+string(filesystem.PathSeparator), ``))
}
