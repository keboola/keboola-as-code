package local

import (
	"fmt"
	"keboola-as-code/src/model"
)

func (m *Manager) UpdatePaths(state model.ObjectState, rename bool) {
	var object model.Object
	if state.HasLocalState() {
		object = state.LocalState()
	} else if state.HasRemoteState() {
		object = state.RemoteState()
	} else {
		panic(fmt.Errorf("object Local or Remote state must be set"))
	}

	switch v := state.(type) {
	case *model.BranchState:
		branch := object.(*model.Branch)

		// Set paths
		if v.Path == "" || rename {
			v.Path = m.Naming().BranchPath(branch, branch.IsDefault)
		}
		v.ResolveParentPath()
	case *model.ConfigState:
		config := object.(*model.Config)

		// Get parent - branch
		branchKey := v.BranchKey()
		branchManifest, found := m.manifest.GetRecord(branchKey)
		if !found {
			panic(fmt.Errorf("branch manifest wit key \"%s\" not found", branchKey))
		}

		// Set paths
		if v.Path == "" || rename {
			v.Path = m.Naming().ConfigPath(v.Component, config)
		}
		v.ResolveParentPath(branchManifest.(*model.BranchManifest))
	case *model.ConfigRowState:
		row := object.(*model.ConfigRow)

		// Get parent - config
		configKey := row.ConfigKey()
		configManifest, found := m.manifest.GetRecord(configKey)
		if !found {
			panic(fmt.Errorf("config manifest wit key \"%s\" not found", configKey))
		}

		// Set paths
		if v.Path == "" || rename {
			v.Path = m.Naming().ConfigRowPath(row)
		}
		v.ResolveParentPath(configManifest.(*model.ConfigManifest))
	default:
		panic(fmt.Errorf(`unexpect type "%T"`, state))
	}
}
