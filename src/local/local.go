package local

import (
	"keboola-as-code/src/model"
)

func LoadState(projectDir string, metadataDir string) (*model.State, error) {
	// Load manifest
	_, err := LoadManifest(metadataDir)
	if err != nil {
		return nil, err
	}

	state := &model.State{}
	return state, nil
}
