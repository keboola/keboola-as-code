package transformation

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// OnObjectPathUpdate - update Blocks/Codes paths.
func (m *transformationMapper) OnObjectPathUpdate(event model.OnObjectPathUpdateEvent) error {
	if ok, err := m.isTransformationConfigState(event.ObjectState); err != nil || !ok {
		return err
	}

	configState := event.ObjectState.(*model.ConfigState)

	// Check if using developer-friendly format (transform.* file).
	// In this format, code is stored in a single file, not in blocks directories,
	// so we skip the path update operations.
	// Use local state if available, otherwise remote state for componentID
	var componentID = configState.ComponentID
	transformPath := m.state.NamingGenerator().TransformFilePath(configState.Path(), componentID)
	if m.state.ObjectsRoot().IsFile(context.Background(), transformPath) {
		return nil
	}

	// Legacy format: Rename transformation blocks/codes directories
	if configState.HasLocalState() {
		for _, block := range configState.Local.Transformation.Blocks {
			m.updateBlockPath(event.PathsGenerator, configState, block)
		}
	}
	if configState.HasRemoteState() {
		for _, block := range configState.Remote.Transformation.Blocks {
			m.updateBlockPath(event.PathsGenerator, configState, block)
		}
	}
	return nil
}

func (m *transformationMapper) updateBlockPath(g model.PathsGenerator, parent *model.ConfigState, block *model.Block) {
	// Update parent path
	oldPath := block.Path()
	blocksDir := m.state.NamingGenerator().BlocksDir(parent.Path())
	block.SetParentPath(blocksDir)

	// Re-generate object path IF rename is enabled OR path is not set
	if block.RelativePath == "" || g.RenameEnabled() {
		renameFrom := block.Path()
		block.AbsPath = m.state.NamingGenerator().BlockPath(block.GetParentPath(), block)

		// Has been block renamed?
		newPath := block.Path()
		if renameFrom != newPath {
			g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}

	// Process codes
	for _, code := range block.Codes {
		m.updateCodePath(g, parent, block, code)
	}
}

func (m *transformationMapper) updateCodePath(g model.PathsGenerator, parent *model.ConfigState, block *model.Block, code *model.Code) {
	// Update parent path
	oldPath := code.Path()
	oldPathCodeFile := m.state.NamingGenerator().CodeFilePath(code)
	code.SetParentPath(block.Path())

	// Re-generate object path IF rename is enabled OR path is not set
	if code.RelativePath == "" || g.RenameEnabled() {
		renameFrom := code.Path()
		code.AbsPath = m.state.NamingGenerator().CodePath(code.GetParentPath(), code)
		// Has been code renamed?
		newPath := code.Path()
		if renameFrom != newPath {
			g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
		}
	}

	// Rename code file
	m.updateCodeFilePath(g, parent, code, oldPathCodeFile)
}

func (m *transformationMapper) updateCodeFilePath(g model.PathsGenerator, parent *model.ConfigState, code *model.Code, oldPath string) {
	renameFrom := m.state.NamingGenerator().CodeFilePath(code)
	code.CodeFileName = m.state.NamingGenerator().CodeFileName(code.ComponentID)
	newPath := m.state.NamingGenerator().CodeFilePath(code)
	if renameFrom != newPath {
		g.AddRenamed(model.RenamedPath{ObjectState: parent, OldPath: oldPath, RenameFrom: renameFrom, NewPath: newPath})
	}
}
