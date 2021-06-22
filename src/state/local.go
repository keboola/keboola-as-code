package state

import (
	"fmt"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

func LoadLocalState(state *State, m *manifest.Manifest, api *remote.StorageApi) {
	var invalidKeys []string
	for _, key := range m.Registry.Keys() {
		item, _ := m.Registry.Get(key)
		switch record := item.(type) {
		// Add branch
		case *manifest.BranchManifest:
			if branch, err := local.LoadBranch(m.ProjectDir, record); err == nil {
				state.SetBranchLocalState(branch, record)
			} else {
				invalidKeys = append(invalidKeys, key)
				state.AddLocalError(err)
			}
		// Add config
		case *manifest.ConfigManifest:
			if config, err := local.LoadConfig(m.ProjectDir, record); err == nil {
				if component, err := getComponent(state, api, config.ComponentId); err == nil {
					state.SetConfigLocalState(component, config, record)
				} else {
					state.AddLocalError(err)
				}
			} else {
				invalidKeys = append(invalidKeys, key)
				state.AddLocalError(err)
			}
		// Add config row
		case *manifest.ConfigRowManifest:
			if row, err := local.LoadConfigRow(m.ProjectDir, record); err == nil {
				state.SetConfigRowLocalState(row, record)
			} else {
				invalidKeys = append(invalidKeys, key)
				state.AddLocalError(err)
			}
		default:
			panic(fmt.Errorf(`unexpected type "%T", key "%s"`, item, key))
		}
	}

	// Delete invalid records
	for _, key := range invalidKeys {
		m.Registry.Delete(key)
	}
}

func getComponent(state *State, api *remote.StorageApi, componentId string) (*model.Component, error) {
	// Load component from state if present
	if component, found := state.GetComponent(componentId); found {
		return component, nil
	}

	// Or by API
	if component, err := api.GetComponent(componentId); err == nil {
		return component, nil
	} else {
		return nil, err
	}
}
