package datagateway

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// AfterRemoteOperation backfills workspace details for data-gateway configs after loading from remote.
func (m *dataGatewayMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	api := m.KeboolaProjectAPI()

	// Process all loaded configs
	for _, objectState := range changes.Loaded() {
		if !m.isDataGatewayConfigKey(objectState.Key()) {
			continue
		}

		// Get the config
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		config := configState.Remote
		if config == nil {
			continue
		}

		// Check if config has an ID (required for listing config workspaces)
		if config.ID == "" {
			continue
		}

		// Check if workspace is already set
		workspaceID, found, _ := config.Content.GetNested("parameters.db.workspaceId")
		if found && workspaceID != nil && workspaceID != "" {
			// Workspace already set, but we can still backfill other details if missing
			m.logger.Debugf(ctx, `Config "%s" already has workspaceId, checking for missing details...`, config.Name)
		}

		// List existing workspaces for this config
		workspaces, err := api.ListConfigWorkspacesRequest(config.BranchID, config.ComponentID, config.ID).Send(ctx)
		if err != nil {
			m.logger.Warnf(ctx, `Cannot list workspaces for config "%s": %s`, config.Name, err.Error())
			continue
		}

		// If no workspaces exist, skip
		if len(*workspaces) == 0 {
			m.logger.Debugf(ctx, `No workspaces found for config "%s"`, config.Name)
			continue
		}

		// Use the first workspace to backfill details
		workspace := (*workspaces)[0]
		m.logger.Debugf(ctx, `Backfilling workspace %d details for config "%s"`, workspace.ID, config.Name)
		backfillWorkspaceDetails(config, workspace)
	}

	return nil
}
