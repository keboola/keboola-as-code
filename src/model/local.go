package model

import (
	"fmt"
	"keboola-as-code/src/utils"
)

func LoadLocalState(projectDir string, metadataDir string) (*State, *PathsState, *utils.Error) {
	// Create structures
	state := NewState()
	paths, err := NewPathsState(projectDir)
	if err != nil {
		panic(err)
	}

	// Load manifest
	manifest, err := LoadManifest(projectDir, metadataDir)
	if err != nil {
		return state, paths, utils.WrapError(err)
	}

	// Add branches
	var branchById = make(map[int]*ManifestBranch)
	for _, b := range manifest.Branches {
		paths.MarkTracked(b.MetaFilePath(projectDir))
		branch, err := b.ToModel(projectDir)
		if err == nil {
			branchById[b.Id] = b
			state.AddBranch(branch)
		} else {
			state.AddError(err)
		}
	}

	// Add configs
	for _, c := range manifest.Configs {
		if b, ok := branchById[c.BranchId]; ok {
			paths.MarkTracked(c.MetaFilePath(b, projectDir))
			paths.MarkTracked(c.ConfigFilePath(b, projectDir))
			config, err := c.ToModel(b, projectDir)
			if err == nil {
				state.AddConfig(config)
			} else {
				state.AddError(err)
			}

			// Add rows to tracked paths
			for _, r := range c.Rows {
				paths.MarkTracked(r.MetaFilePath(b, c, projectDir))
				paths.MarkTracked(r.ConfigFilePath(b, c, projectDir))
			}
		} else {
			state.AddError(fmt.Errorf("b \"%d\" not found - referenced from the config \"%s:%s\" in \"%s\"", c.BranchId, c.ComponentId, c.Id, manifest.path))
		}
	}

	// Merge errors
	for _, err := range paths.Error().Errors() {
		state.AddError(err)
	}
	return state, paths, state.Error()
}
