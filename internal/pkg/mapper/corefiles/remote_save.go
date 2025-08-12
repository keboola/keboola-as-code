package corefiles

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave ensures configurations with rows trigger row synchronization
func (m *coreFilesMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	// Only handle Config objects
	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}

	// Check if there are rows in the remote state that should be preserved
	configState, ok := m.state.Get(config.Key())
	if !ok {
		return nil
	}

	if configState.HasRemoteState() {
		if remoteConfig, ok := configState.RemoteState().(*model.ConfigWithRows); ok && len(remoteConfig.Rows) > 0 {
			// Add "rows" to changed fields to trigger SDK row synchronization
			recipe.ChangedFields.Add("rows")
		}
	}

	return nil
}
