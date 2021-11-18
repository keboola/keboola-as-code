package orchestrator

import "github.com/keboola/keboola-as-code/internal/pkg/model"

// OnObjectPathUpdate - update Phases/Tasks paths.
func (m *orchestratorMapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	if ok, err := m.isOrchestratorConfigKey(event.ObjectState.Key()); err != nil || !ok {
		return err
	}

	// Rename orchestrator phases/tasks
	configState := event.ObjectState.(*model.ConfigState)
	if configState.HasLocalState() {
		for _, phase := range configState.Local.Orchestration.Phases {
			m.updatePhasePath(event.PathsGenerator, configState, phase)
		}
	}
	if configState.HasRemoteState() {
		for _, phase := range configState.Remote.Orchestration.Phases {
			m.updatePhasePath(event.PathsGenerator, configState, phase)
		}
	}
	return nil
}

func (m *orchestratorMapper) updatePhasePath(g model.PathsGenerator, parent *model.ConfigState, phase *model.Phase) {
	// Update parent path
	oldPath := phase.Path()
	phasesDir := m.Naming.PhasesDir(parent.Path())
	phase.SetParentPath(phasesDir)

	// Re-generate object path IF rename is enabled OR path is not set
	if phase.ObjectPath == "" || g.RenameEnabled() {
		renameFrom := phase.Path()
		phase.PathInProject = m.Naming.PhasePath(phase.GetParentPath(), phase)

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
	if task.ObjectPath == "" || g.RenameEnabled() {
		renameFrom := task.Path()
		task.PathInProject = m.Naming.TaskPath(task.GetParentPath(), task)
		// Has been task renamed?
		newPath := task.Path()
		if renameFrom != newPath {
			g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}
}
