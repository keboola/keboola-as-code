package state

import (
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

// LoadLocalState - manifest -> local files -> unified model
func LoadLocalState(state *State, projectDir string, content *manifest.Content, api *remote.StorageApi) {
	for _, b := range content.Branches {
		// Add branch
		if branch, err := local.LoadBranch(projectDir, b); err == nil {
			state.SetBranchLocalState(branch, b)
		} else {
			b.SetInvalid()
			state.AddLocalError(err)
		}
	}
	for _, c := range content.Configs {
		// Add config
		if config, err := local.LoadConfig(projectDir, c.ConfigManifest); err == nil {
			if component, err := getComponent(state, api, config.ComponentId); err == nil {
				state.SetConfigLocalState(component, config, c.ConfigManifest)
			} else {
				state.AddLocalError(err)
			}
		} else {
			c.SetInvalid()
			state.AddLocalError(err)
		}

		// Rows
		for _, r := range c.Rows {
			if row, err := local.LoadConfigRow(projectDir, r); err == nil {
				state.SetConfigRowLocalState(row, r)
			} else {
				r.SetInvalid()
				state.AddLocalError(err)
			}
		}
	}
}

func getComponent(state *State, api *remote.StorageApi, componentId string) (*model.Component, error) {
	// Load component from state if present
	if component := state.GetComponent(model.ComponentKey{Id: componentId}); component != nil {
		return component, nil
	}

	// Or by API
	if component, err := api.GetComponent(componentId); err == nil {
		state.setComponent(component)
		return component, nil
	} else {
		return nil, err
	}
}
