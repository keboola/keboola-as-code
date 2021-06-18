package state

import (
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

func LoadLocalState(state *model.State, projectDir, metadataDir string) *utils.Error {
	// Load manifest
	manifest, err := model.LoadManifest(projectDir, metadataDir)
	if err != nil {
		return utils.WrapError(err)
	}

	// Add branches
	for _, branchManifest := range manifest.Branches {
		branch, err := branchManifest.ToModel(projectDir)
		if err != nil {
			state.AddLocalError(err)
			continue
		}
		state.SetBranchLocalState(branch, branchManifest)
	}

	// Add configs
	for _, configManifest := range manifest.Configs {
		config, err := configManifest.ToModel(projectDir)
		if err != nil {
			state.AddLocalError(err)
			continue
		}
		state.SetConfigLocalState(config, configManifest)

		// Add rows
		for _, rowManifest := range configManifest.Rows {
			row, err := rowManifest.ToModel(projectDir)
			if err != nil {
				state.AddLocalError(err)
				continue
			}
			config.Rows = append(config.Rows, row)
			state.SetConfigRowLocalState(row, rowManifest)
		}
		config.SortRows()
	}

	return state.LocalErrors()
}
