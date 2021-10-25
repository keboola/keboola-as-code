package local

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *Manager) UpdatePaths(state model.ObjectState, rename bool) error {
	object := state.LocalOrRemoteState()

	// Update parent path
	if err := m.manifest.ResolveParentPath(state.Manifest()); err != nil {
		return err
	}

	// Re-generate object path IF rename is enabled OR path is not set
	if state.GetObjectPath() == "" || rename {
		switch v := state.(type) {
		case *model.BranchState:
			v.PathInProject = m.Naming().BranchPath(object.(*model.Branch))
		case *model.ConfigState:
			config := object.(*model.Config)
			if component, err := m.state.Components().Get(*config.ComponentKey()); err == nil {
				v.PathInProject = m.Naming().ConfigPath(v.GetParentPath(), component, config)
			} else {
				return err
			}
		case *model.ConfigRowState:
			row := object.(*model.ConfigRow)
			if component, err := m.state.Components().Get(*row.ComponentKey()); err == nil {
				v.PathInProject = m.Naming().ConfigRowPath(v.GetParentPath(), component, row)
			} else {
				return err
			}
		default:
			panic(fmt.Errorf(`unexpect type "%T"`, state))
		}
	}

	return nil
}

func (m *Manager) UpdateBlockPath(block *model.Block, configDir string, rename bool) {
	// Update parent path
	blocksDir := m.Naming().BlocksDir(configDir)
	block.SetParentPath(blocksDir)

	// Re-generate object path IF rename is enabled OR path is not set
	if block.ObjectPath == "" || rename {
		block.PathInProject = m.Naming().BlockPath(block.GetParentPath(), block)
	}
}

func (m *Manager) UpdateCodePath(block *model.Block, code *model.Code, rename bool) {
	// Update parent path
	code.SetParentPath(block.Path())

	// Re-generate object path IF rename is enabled OR path is not set
	if block.ObjectPath == "" || rename {
		code.PathInProject = m.Naming().CodePath(code.GetParentPath(), code)
	}
}
