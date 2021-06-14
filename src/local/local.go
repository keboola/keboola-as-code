package local

import (
	"fmt"
	"keboola-as-code/src/model"
)

func LoadState(projectDir string, metadataDir string) (*model.State, error) {
	state := model.NewState()

	// Load manifest
	manifest, err := LoadManifest(metadataDir)
	if err != nil {
		return nil, err
	}

	// Add branches
	var branchById = make(map[int]*Branch)
	for _, b := range manifest.Branches {
		branch, err := b.ToModel(projectDir)
		if err != nil {
			return nil, err
		}
		branchById[b.Id] = b
		if err := state.AddBranch(branch); err != nil {
			return nil, err
		}
	}

	// Add configs
	for _, c := range manifest.Configs {
		branch, ok := branchById[c.BranchId]
		if !ok {
			return nil, fmt.Errorf("branch \"%d\" not found - referenced from the config \"%s:%s\" in \"%s\"", c.BranchId, c.ComponentId, c.Id, manifest.path)
		}

		config, err := c.ToModel(branch, projectDir)
		if err != nil {
			return nil, err
		}

		if err := state.AddConfig(config); err != nil {
			return nil, err
		}
	}

	return state, nil
}
