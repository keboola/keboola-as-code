package datagateway

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// AfterRemoteOperation handles workspace creation and backfilling for data-gateway configs after remote operations.
// It creates workspaces for newly saved configs and backfills workspace details for loaded configs.
func (m *dataGatewayMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	localWork := m.state.LocalManager().NewUnitOfWork(ctx)
	localChangesScheduled := false
	scheduleLocalSave := func(configState *model.ConfigState) {
		if configState.Local == nil {
			return
		}
		localWork.SaveObject(configState, configState.Local, model.NewChangedFields("configuration"))
		localChangesScheduled = true
	}

	errs := errors.NewMultiError()

	// Process saved configs - create workspaces for configs that were just created/updated
	for _, objectState := range changes.Saved() {
		if !m.isDataGatewayConfigKey(objectState.Key()) {
			continue
		}

		// Get the config
		configState, ok := objectState.(*model.ConfigState)
		if !ok {
			continue
		}

		// Use remote state since it was just saved and has the latest ID
		config := configState.Remote
		if config == nil {
			continue
		}

		// Check if config has an ID (required for creating config workspace)
		if config.ID == "" {
			continue
		}

		// Check if workspace is already set
		workspaceID, found, _ := config.Content.GetNested("parameters.db.workspaceId")
		if found && workspaceID != nil && workspaceID != "" {
			// Workspace already set, skip
			m.logger.Debugf(ctx, `Config "%s" already has workspaceId %v`, config.Name, workspaceID)
			continue
		}

		// Workspace is missing, ensure one exists
		// This handles the case where a new config was created and pushed, but workspace wasn't created yet
		m.logger.Debugf(ctx, `Config "%s" is missing workspaceId after save, ensuring workspace exists...`, config.Name)
		if err := m.ensureWorkspaceForConfig(ctx, config); err != nil {
			errs.Append(errors.PrefixErrorf(err, `cannot ensure workspace for config "%s"`, config.Name))
			continue
		}

		// Check if workspace was created and details were set
		workspaceID, found, _ = config.Content.GetNested("parameters.db.workspaceId")
		if found && workspaceID != nil && workspaceID != "" {
			// Update the configuration in remote with workspace details
			// If this update fails, the workspace exists but the config doesn't reference it.
			// This creates an inconsistent state, so we return the error to prevent silent failures.
			api := m.KeboolaProjectAPI()
			changedFields := model.NewChangedFields()
			changedFields.Add("configuration")
			apiObject, apiChangedFields := config.ToAPIObject("Workspace created and configuration updated", changedFields)
			_, err := api.UpdateRequest(apiObject, apiChangedFields).Send(ctx)
			if err != nil {
				// Return error instead of logging and continuing.
				// The workspace has been created and should be used, but the config update failed.
				// This error must be propagated to prevent inconsistent state.
				errs.Append(errors.Errorf(`cannot update configuration "%s" with workspace details: %w`, config.Name, err))
				continue
			}
			m.logger.Debugf(ctx, `Updated configuration "%s" with workspace details`, config.Name)

			// Update remote state with the updated config
			configState.SetRemoteState(config)

			// Sync workspace details to local state if it exists
			if configState.Local != nil {
				_ = configState.Local.Content.SetNested("parameters.db.workspaceId", workspaceID)
				normalizeWorkspaceID(configState.Local)
				// Also sync other workspace details
				if host, found, _ := config.Content.GetNested("parameters.db.host"); found {
					_ = configState.Local.Content.SetNested("parameters.db.host", host)
				}
				if user, found, _ := config.Content.GetNested("parameters.db.user"); found {
					_ = configState.Local.Content.SetNested("parameters.db.user", user)
				}
				if database, found, _ := config.Content.GetNested("parameters.db.database"); found {
					_ = configState.Local.Content.SetNested("parameters.db.database", database)
				}
				scheduleLocalSave(configState)
			}
		}
	}

	// Process loaded configs - backfill workspace details
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
		api := m.KeboolaProjectAPI()
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
		if backfillWorkspaceDetails(config, workspace) {
			m.logger.Debugf(ctx, `Backfilled workspace %d details for config "%s"`, workspace.ID, config.Name)
		}
		if configState.Local != nil && backfillWorkspaceDetails(configState.Local, workspace) {
			scheduleLocalSave(configState)
		}
	}

	if localChangesScheduled {
		if err := localWork.Invoke(); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

// MapAfterRemoteLoad normalizes remote configs so diff sees current workspace details.
func (m *dataGatewayMapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	if recipe == nil {
		return nil
	}

	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}
	if !m.isDataGatewayConfigKey(config.Key()) {
		return nil
	}
	if config.ID == "" {
		return nil
	}
	normalizeWorkspaceID(config)
	if !needsWorkspaceDetails(config) {
		m.logger.Debugf(ctx, `Config "%s" already has complete workspace details during remote load`, config.Name)
		return nil
	}
	m.logger.Debugf(ctx, `Config "%s" is missing workspace details during remote load, fetching workspace list`, config.Name)

	api := m.KeboolaProjectAPI()
	workspaces, err := api.ListConfigWorkspacesRequest(config.BranchID, config.ComponentID, config.ID).Send(ctx)
	if err != nil {
		m.logger.Warnf(ctx, `Cannot list workspaces for config "%s": %s`, config.Name, err.Error())
		return nil
	}
	if len(*workspaces) == 0 {
		m.logger.Debugf(ctx, `No workspaces found for config "%s" during remote load`, config.Name)
		return nil
	}

	workspace := (*workspaces)[0]
	if backfillWorkspaceDetails(config, workspace) {
		m.logger.Debugf(ctx, `Backfilled workspace %d details for config "%s" during remote load`, workspace.ID, config.Name)
	}

	return nil
}

// MapAfterLocalLoad normalizes local configs to keep workspaceId types consistent with remote state.
func (m *dataGatewayMapper) MapAfterLocalLoad(ctx context.Context, recipe *model.LocalLoadRecipe) error {
	if recipe == nil {
		return nil
	}

	config, ok := recipe.Object.(*model.Config)
	if !ok {
		return nil
	}
	if !m.isDataGatewayConfigKey(config.Key()) {
		return nil
	}

	normalizeWorkspaceID(config)

	return nil
}
