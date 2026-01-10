package orchestrator

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// OnObjectPathUpdate - update Phases/Tasks paths.
// Phases and tasks are now stored inline in _config.yml, so no path updates are needed.
func (m *orchestratorMapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	// Phases and tasks are now stored inline in _config.yml.
	// No directory structure is used for phases/tasks, so skip path updates.
	return nil
}

func (m *orchestratorMapper) updatePhasePath(g model.PathsGenerator, parent *model.ConfigState, phase *model.Phase) {
	// Update parent path
	oldPath := phase.Path()
	phasesDir := m.state.NamingGenerator().PhasesDir(parent.Path())
	phase.SetParentPath(phasesDir)

	// Re-generate object path IF rename is enabled OR path is not set
	if phase.RelativePath == "" || g.RenameEnabled() {
		renameFrom := phase.Path()
		phase.AbsPath = m.state.NamingGenerator().PhasePath(phase.GetParentPath(), phase)

		// Has been phase renamed?
		newPath := phase.Path()
		if renameFrom != newPath {
			g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}

	// Process tasks
	for _, task := range phase.Tasks {
		m.updateTaskPath(g, parent, phase, task)
	}
}

func (m *orchestratorMapper) updateTaskPath(g model.PathsGenerator, parent *model.ConfigState, phase *model.Phase, task *model.Task) {
	// Update parent path
	oldPath := task.Path()
	task.SetParentPath(phase.Path())

	// Re-generate object path IF rename is enabled OR path is not set
	if task.RelativePath == "" || g.RenameEnabled() {
		renameFrom := task.Path()
		task.AbsPath = m.state.NamingGenerator().TaskPath(task.GetParentPath(), task)
		// Has been task renamed?
		newPath := task.Path()
		if renameFrom != newPath {
			g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}
}
