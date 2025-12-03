package datagateway

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// MapBeforeRemoteSave ensures a workspace exists for data-gateway configs before saving to remote.
func (m *dataGatewayMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	// Object must be data-gateway config
	if !m.isDataGatewayConfigKey(recipe.Object.Key()) {
		return nil
	}

	config := recipe.Object.(*model.Config)

	// Check if workspace is already set
	workspaceID, found, _ := config.Content.GetNested("parameters.db.workspaceId")
	if found && workspaceID != nil && workspaceID != "" {
		// Workspace already exists, nothing to do
		m.logger.Debugf(ctx, `Config "%s" already has workspaceId %v`, config.Name, workspaceID)
		return nil
	}

	// Workspace is missing, ensure one exists
	m.logger.Debugf(ctx, `Config "%s" is missing workspaceId, ensuring workspace exists...`, config.Name)
	if err := m.ensureWorkspaceForConfig(ctx, config); err != nil {
		return err
	}

	// Mark configuration as changed so it gets sent to the API
	recipe.ChangedFields.Add("configuration")

	return nil
}
