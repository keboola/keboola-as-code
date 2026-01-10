package orchestrator

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// OnObjectPathUpdate - update Phases/Tasks paths.
func (m *orchestratorMapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	if ok, err := m.isOrchestratorConfigKey(event.ObjectState.Key()); err != nil || !ok {
		return err
	}

	configState := event.ObjectState.(*model.ConfigState)

	// Check if using developer-friendly format (pipeline.yml).
	// In this format, phases/tasks are stored inline in the YAML file,
	// not in separate directories, so we skip the path update operations.
	pipelinePath := m.state.NamingGenerator().PipelineFilePath(configState.Path())
	if m.state.ObjectsRoot().IsFile(context.Background(), pipelinePath) {
		return nil
	}

	// Legacy format: Rename orchestrator phases/tasks directories
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
