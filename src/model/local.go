package model

import (
	"fmt"
	"keboola-as-code/src/utils"
)

func LoadLocalState(state *State) *utils.Error {
	// Load manifest
	projectDir := state.ProjectDir()
	metadataDir := state.MetadataDir()
	manifest, err := LoadManifest(projectDir, metadataDir)
	if err != nil {
		return utils.WrapError(err)
	}

	// Add branches
	var branchById = make(map[int]*ManifestBranch)
	for _, b := range manifest.Branches {
		branch, err := b.ToModel(projectDir)
		if err != nil {
			state.AddLocalError(err)
			continue
		}

		branchById[b.Id] = b
		state.SetBranchLocalState(branch, b, b.MetaFilePath(projectDir))
	}

	// Add configs
	for _, c := range manifest.Configs {
		b, ok := branchById[c.BranchId]
		if !ok {
			state.AddLocalError(fmt.Errorf("b \"%d\" not found - referenced from the config \"%s:%s\" in \"%s\"", c.BranchId, c.ComponentId, c.Id, manifest.path))
			continue
		}

		config, err := c.ToModel(b, projectDir)
		if err != nil {
			state.AddLocalError(err)
			continue
		}
		state.SetConfigLocalState(config, c, c.MetaFilePath(b, projectDir), c.ConfigFilePath(b, projectDir))

		// Add rows
		for _, r := range c.Rows {
			row, err := r.ToModel(b, c, projectDir)
			if err != nil {
				state.AddLocalError(err)
				continue
			}

			config.Rows = append(config.Rows, row)
			state.SetConfigRowLocalState(row, r, r.MetaFilePath(b, c, projectDir), r.ConfigFilePath(b, c, projectDir))
		}
		config.SortRows()
	}

	return state.LocalErrors()
}
