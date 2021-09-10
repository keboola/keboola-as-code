package local

import (
	"fmt"

	"keboola-as-code/src/model"
)

func (m *Manager) UpdatePaths(state model.ObjectState, rename bool) {
	object := state.LocalOrRemoteState()

	// Update parent path
	m.manifest.ResolveParentPath(state.Manifest())

	// Re-generate object path IF rename is enabled OR path is not set
	if state.GetObjectPath() == "" || rename {
		switch v := state.(type) {
		case *model.BranchState:
			v.PathInProject = m.Naming().BranchPath(object.(*model.Branch))
		case *model.ConfigState:
			config := object.(*model.Config)
			v.PathInProject = m.Naming().ConfigPath(v.ParentPath, v.Component, config)
		case *model.ConfigRowState:
			row := object.(*model.ConfigRow)
			v.PathInProject = m.Naming().ConfigRowPath(v.ParentPath, row)
		default:
			panic(fmt.Errorf(`unexpect type "%T"`, state))
		}
	}
}

func (m *Manager) UpdateBlockPath(block *model.Block, rename bool) {
	// Update parent path
	configDir := m.manifest.MustGetRecord(block.ConfigKey()).RelativePath()
	blocksDir := m.Naming().BlocksDir(configDir)
	block.SetParentPath(blocksDir)
	if !rename {
		return
	}

	// Re-generate object path
	block.PathInProject = m.Naming().BlockPath(block.ParentPath, block)
}

func (m *Manager) UpdateCodePath(block *model.Block, code *model.Code, rename bool) {
	// Update parent path
	code.SetParentPath(block.RelativePath())
	if !rename {
		return
	}

	// Re-generate object path
	code.PathInProject = m.Naming().CodePath(code.ParentPath, code)
}
