package orchestrator

import (
	"fmt"
	"sort"
	"strings"

	"github.com/iancoleman/orderedmap"
	"v.io/x/lib/toposort"

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
			errors.Append(err)
		}
	}
	return errors.ErrorOrNil()
}

func (m *orchestratorMapper) loadLocalPhases(config *model.Config) error {
	manifest := m.State.MustGet(config.ConfigKey).Manifest().(*model.ConfigManifest)
	loader := &localLoader{
		MapperContext: m.MapperContext,
		branch:        m.State.MustGet(config.BranchKey()).(*model.BranchState),
		config:        config,
		manifest:      manifest,
		phasesDir:     m.Naming.PhasesDir(manifest.Path()),
		phasesByPath:  make(map[string]*model.Phase),
		dependsOnMap:  make(map[string][]string),
		errors:        utils.NewMultiError(),
	}
	return loader.load()
}

type localLoader struct {
	model.MapperContext
	branch       *model.BranchState
	config       *model.Config
	manifest     *model.ConfigManifest
	phasesDir    string
	phases       []*model.Phase
	phasesByPath map[string]*model.Phase // phase path -> struct
	dependsOnMap map[string][]string     // phase path -> depends on phases paths
	errors       *utils.Error
}

func (l *localLoader) load() error {
	// Load phases and tasks from filesystem
	for phaseIndex, phaseDir := range l.phasesDirs() {
		phase := l.addPhase(phaseIndex, phaseDir)
		if phase != nil {
			for taskIndex, taskDir := range l.tasksDirs(phase) {
				l.addTask(taskIndex, phase, taskDir)
			}
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

func (l *localLoader) addPhase(phaseIndex int, path string) *model.Phase {
	// Create strict
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

	// Load phase config
	file := l.loadJsonFile(l.Naming.PhaseFilePath(*phase), `phase config`)
	if file == nil {
		return nil
	}
	phaseContent := file.Content
	errors := utils.NewMultiError()

	// Get name
	if err := l.parsePhaseName(phase, phaseContent); err != nil {
		errors.Append(err)
	}

	// Get dependsOn
	if err := l.parsePhaseDependsOn(phase, phaseContent); err != nil {
		errors.Append(err)
	}

	// Set additional content
	phase.Content = phaseContent

	// Handle errors
	if errors.Len() > 0 {
		l.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid config "%s"`, file.Path), errors))
		return nil
	}

	l.phases = append(l.phases, phase)
	l.phasesByPath[phase.ObjectPath] = phase
	return phase
}

func (l *localLoader) addTask(taskIndex int, phase *model.Phase, path string) {
	// Create struct
	task := model.Task{
		TaskKey: model.TaskKey{
			Index: taskIndex,
		},
		PathInProject: model.NewPathInProject(
			phase.Path(),
			path,
		),
	}

	// Track task path
	l.manifest.AddRelatedPath(task.Path())

	// Load task config
	file := l.loadJsonFile(l.Naming.TaskFilePath(task), `task config`)
	if file == nil {
		return
	}
	taskContent := file.Content
	errors := utils.NewMultiError()

	// Get name
	if err := l.parseTaskName(&task, taskContent); err != nil {
		errors.Append(err)
	}

	// Get target config
	if err := l.parseTaskConfigPath(&task, taskContent); err != nil {
		errors.Append(err)
	}

	// Set additional content
	task.Content = taskContent

	// Handle errors
	if errors.Len() > 0 {
		l.errors.Append(utils.PrefixError(fmt.Sprintf(`invalid config "%s"`, file.Path), errors))
		return
	}

	phase.Tasks = append(phase.Tasks, task)
}

func (l *localLoader) sortPhases() ([]*model.Phase, error) {
	errors := utils.NewMultiError()
	graph := &toposort.Sorter{}

	// Add dependencies to graph
	for _, phase := range l.phases {
		graph.AddNode(phase.ObjectPath)
		for _, dependsOnPath := range l.dependsOnMap[phase.ObjectPath] {
			if l.phasesByPath[dependsOnPath] != nil {
				graph.AddEdge(phase.ObjectPath, dependsOnPath)
			}
		}
	}

	// Topological sort by dependencies
	order, cycles := graph.Sort()
	if len(cycles) > 0 {
		err := utils.NewMultiError()
		err.Append(fmt.Errorf(`found cycles in phases "dependsOn" in "%s"`, l.phasesDir))
		for _, cycle := range cycles {
			var items []string
			for _, item := range cycle {
				items = append(items, `"`+item.(string)+`"`)
			}
			err.AppendRaw(`  - ` + strings.Join(items, ` -> `))
		}
		errors.Append(err)
	}

	// Generate slice
	var phases []*model.Phase
	for phaseIndex, pathRaw := range order {
		path := pathRaw.(string)
		phase := l.phasesByPath[path]
		phase.Index = phaseIndex
		for taskIndex, task := range phase.Tasks {
			task.TaskKey = model.TaskKey{
				PhaseKey: phase.PhaseKey,
				Index:    taskIndex,
			}
			phase.Tasks[taskIndex] = task
		}
		phases = append(phases, phase)
	}

	// Fill in "dependsOn"
	for _, pathRaw := range order {
		var dependsOn []*model.Phase
		path := pathRaw.(string)
		phase := l.phasesByPath[path]
		for _, dependsOnPath := range l.dependsOnMap[path] {
			dependsOnPhase, found := l.phasesByPath[dependsOnPath]
			if !found {
				errors.Append(fmt.Errorf(`missing phase "%s", referenced from "%s"`, dependsOnPath, phase.Path()))
				continue
			}
			dependsOn = append(dependsOn, dependsOnPhase)
		}

		// Sort dependsOn phases
		sort.SliceStable(dependsOn, func(i, j int) bool {
			return dependsOn[i].Index < dependsOn[j].Index
		})

		// Convert ID -> PhaseKey (index)
		phase.DependsOn = make([]model.PhaseKey, 0)
		for _, depPhase := range dependsOn {
			phase.DependsOn = append(phase.DependsOn, depPhase.PhaseKey)
		}
	}

	return phases, errors.ErrorOrNil()
}

func (l *localLoader) parsePhaseName(phase *model.Phase, phaseContent *orderedmap.OrderedMap) error {
	nameRaw, found := phaseContent.Get(`name`)
	if !found {
		return fmt.Errorf(`missing phase[%d] "name" key`, phase.Index)
	}

	name, ok := nameRaw.(string)
	if !ok {
		return fmt.Errorf(`phase[%d] "name" must be string, found %T`, phase.Index, nameRaw)
	}

	phase.Name = name
	phaseContent.Delete(`name`)
	return nil
}

func (l *localLoader) parsePhaseDependsOn(phase *model.Phase, phaseContent *orderedmap.OrderedMap) error {
	var dependsOnSliceRaw []interface{}
	dependsOnValueRaw, found := phaseContent.Get(`dependsOn`)
	if found {
		if v, ok := dependsOnValueRaw.([]interface{}); ok {
			dependsOnSliceRaw = v
		}
	}
	phaseContent.Delete(`dependsOn`)

	// Convert []interface{} -> []string
	var dependsOnPaths []string
	for i, item := range dependsOnSliceRaw {
		if v, ok := item.(string); ok {
			dependsOnPaths = append(dependsOnPaths, v)
		} else {
			return fmt.Errorf(`"dependsOn" key must contain only strings, found %T, index %d`, item, i)
		}
	}

	// Store "dependsOn" to map -> for sorting
	l.dependsOnMap[phase.ObjectPath] = dependsOnPaths
	return nil
}

func (l *localLoader) parseTaskName(task *model.Task, taskContent *orderedmap.OrderedMap) error {
	nameRaw, found := taskContent.Get(`name`)
	if !found {
		return fmt.Errorf(`missing task[%d] "name" key`, task.Index)
	}

	name, ok := nameRaw.(string)
	if !ok {
		return fmt.Errorf(`task[%d] "name" must be string, found %T`, task.Index, nameRaw)
	}

	task.Name = name
	taskContent.Delete(`name`)
	return nil
}

func (l *localLoader) parseTaskConfigPath(task *model.Task, taskContent *orderedmap.OrderedMap) error {
	targetRaw, found := taskContent.Get(`task`)
	if !found {
		return fmt.Errorf(`missing "task" key in task[%d] "%s"`, task.Index, task.Name)
	}

	target, ok := targetRaw.(orderedmap.OrderedMap)
	if !ok {
		return fmt.Errorf(`"task" key must be object, found %T, in task[%d] "%s"`, targetRaw, task.Index, task.Name)
	}

	// Get target config path
	configPathRaw, found := target.Get(`configPath`)
	if !found {
		return fmt.Errorf(`missing "task.configPath" key in task[%d] "%s"`, task.Index, task.Name)
	}
	configPath, ok := configPathRaw.(string)
	if !ok {
		return fmt.Errorf(`"task.configPath" key must be string, found %T, in task[%d] "%s"`, configPathRaw, task.Index, task.Name)
	}
	target.Delete(`configPath`)
	taskContent.Set(`task`, target)

	// Get target config
	configPath = filesystem.Join(l.branch.Path(), configPath)
	configKeyRaw, found := l.Naming.FindByPath(configPath)
	if !found {
		return fmt.Errorf(`config "%s" not found, referenced from task[%d] "%s"`, configPath, task.Index, task.Name)
	}
	configKey, ok := configKeyRaw.(model.ConfigKey)
	if !ok {
		return fmt.Errorf(`path "%s" must be config, found %s, referenced from task[%d] "%s"`, configPath, configKeyRaw.Kind().String(), task.Index, task.Name)
	}
	task.ComponentId = configKey.ComponentId
	task.ConfigId = configKey.Id
	return nil
}

func (l *localLoader) loadJsonFile(path, desc string) *filesystem.JsonFile {
	if file, err := l.Fs.ReadJsonFile(path, desc); err != nil {
		l.errors.Append(err)
		return nil
	} else {
		l.manifest.AddRelatedPath(path)
		return file
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
