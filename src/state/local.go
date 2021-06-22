package state

import (
	"fmt"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

func LoadLocalState(state *model.State, manifest *model.Manifest, api *remote.StorageApi) {
	var invalidKeys []string
	for _, key := range manifest.Registry.Keys() {
		item, _ := manifest.Registry.Get(key)
		switch itemManifest := item.(type) {
		// Add branch
		case *model.BranchManifest:
			if branch, err := itemManifest.ToModel(manifest); err == nil {
				state.SetBranchLocalState(branch, itemManifest)
			} else {
				invalidKeys = append(invalidKeys, key)
				state.AddLocalError(err)
			}
		// Add config
		case *model.ConfigManifest:
			if config, err := itemManifest.ToModel(manifest); err == nil {
				if component, err := getComponent(state, api, config.ComponentId); err == nil {
					state.SetConfigLocalState(component, config, itemManifest)
				} else {
					state.AddLocalError(err)
				}
			} else {
				invalidKeys = append(invalidKeys, key)
				state.AddLocalError(err)
			}
		// Add config row
		case *model.ConfigRowManifest:
			if row, err := itemManifest.ToModel(manifest); err == nil {
				state.SetConfigRowLocalState(row, itemManifest)
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
		manifest.Registry.Delete(key)
	}
}

func getComponent(state *model.State, api *remote.StorageApi, componentId string) (*model.Component, error) {
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
