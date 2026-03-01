package remote

import (
	"context"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// loadNotifications loads notification subscriptions from API and matches them to configs.
// For each notification, we determine the parent config by matching filters (job.configuration.id).
func (u *UnitOfWork) loadNotifications(ctx context.Context) error {
	// Load all notification subscriptions from API
	subscriptions, err := u.keboolaProjectAPI.
		ListNotificationSubscriptionsRequest().
		Send(ctx)
	if err != nil {
		return err
	}

	// Build map of config ID -> config manifest for quick lookup
	configsByID := make(map[keboola.ConfigID]*model.ConfigManifest)
	for _, record := range u.Manifest().All() {
		if configManifest, ok := record.(*model.ConfigManifest); ok {
			configsByID[configManifest.ID] = configManifest
		}
	}

	// Load each notification from API
	for _, apiSub := range *subscriptions {
		// Find which config this notification belongs to by checking filters
		var parentConfig *model.ConfigManifest
		for _, filter := range apiSub.Filters {
			if filter.Field == "job.configuration.id" {
				// Found the config ID filter - look up the config
				configID := keboola.ConfigID(filter.Value)
				if cfg, found := configsByID[configID]; found {
					parentConfig = cfg
					break
				}
			}
		}

		// Skip if we don't have this config locally
		if parentConfig == nil {
			continue
		}

		// Create notification with parent config info
		notification := model.NewNotification(apiSub)
		notification.BranchID = parentConfig.BranchID
		notification.ComponentID = parentConfig.ComponentID
		notification.ConfigID = parentConfig.ID

		// Use loadObject pattern (same as config rows) to properly create manifest and state
		if err := u.loadObject(notification); err != nil {
			return err
		}

		// Get the notification state that was just created
		notificationState, found := u.state.Get(notification.Key())
		if !found {
			continue
		}

		// Set parent path on the manifest
		notificationState.Manifest().SetParentPath(parentConfig.Path())
	}

	return nil
}

// createNotificationRequest builds an API request to create a notification subscription.
// Auto-populates standard filters (branch.id, job.component.id, job.configuration.id) from parent context,
// then merges with user-specified filters.
func (u *UnitOfWork) createNotificationRequest(notification *model.Notification) *keboola.CreateNotificationSubscriptionRequestBuilder {
	req := u.keboolaProjectAPI.
		NewCreateNotificationSubscriptionRequest(
			notification.Event,
			notification.Recipient.Channel,
			notification.Recipient.Address,
		)

	// Auto-populate standard filters from parent context (only non-empty values)
	var autoFilters []keboola.NotificationFilter

	branchIDStr := notification.BranchID.String()
	if branchIDStr != "" && branchIDStr != "0" {
		autoFilters = append(autoFilters, keboola.NotificationFilter{
			Field:    "branch.id",
			Operator: keboola.NotificationFilterOperatorEquals,
			Value:    branchIDStr,
		})
	}

	componentIDStr := string(notification.ComponentID)
	if componentIDStr != "" {
		autoFilters = append(autoFilters, keboola.NotificationFilter{
			Field:    "job.component.id",
			Operator: keboola.NotificationFilterOperatorEquals,
			Value:    componentIDStr,
		})
	}

	configIDStr := string(notification.ConfigID)
	if configIDStr != "" {
		autoFilters = append(autoFilters, keboola.NotificationFilter{
			Field:    "job.configuration.id",
			Operator: keboola.NotificationFilterOperatorEquals,
			Value:    configIDStr,
		})
	}

	// Merge auto-populated filters with user-specified filters
	allFilters := append(autoFilters, notification.Filters...)

	if len(allFilters) > 0 {
		req = req.WithFilters(allFilters)
	}

	//nolint:godox // TODO: Handle expiration - NotificationExpiration doesn't expose fields
	// Need SDK update to support passing expiration directly
	_ = notification.ExpiresAt

	//nolint:godox // TODO: Add config reference once SDK supports it
	// req = req.WithConfig(notification.BranchID, notification.ComponentID, notification.ConfigID)

	return req
}

// buildNotificationCreateRequest wraps createNotificationRequest with success/error callbacks
// that update the local state and registry after the API call completes.
func (u *UnitOfWork) buildNotificationCreateRequest(
	notificationState *model.NotificationState,
	notification *model.Notification,
) request.APIRequest[*keboola.NotificationSubscription] {
	return u.createNotificationRequest(notification).Build().
		WithOnError(func(_ context.Context, err error) error {
			errMsg := err.Error()
			if strings.Contains(errMsg, "400") || strings.Contains(errMsg, "Bad Request") {
				return errors.Errorf(
					`failed to create notification "%s": %w. `+
						`This may be caused by invalid filter field names. `+
						`Ensure filters use correct field names like "job.configuration.id", "branch.id", "job.component.id"`,
					notification.ID, err,
				)
			}
			return err
		}).
		WithOnSuccess(func(_ context.Context, created *keboola.NotificationSubscription) error {
			// Create remote notification from API response (has all auto-populated filters)
			remoteNotification := model.NewNotification(created)
			remoteNotification.BranchID = notification.BranchID
			remoteNotification.ComponentID = notification.ComponentID
			remoteNotification.ConfigID = notification.ConfigID

			// Update local notification with API-assigned ID and auto-populated filters
			notification.ID = created.ID
			notification.CreatedAt = created.CreatedAt
			notification.Filters = created.Filters

			// Before updating the manifest ID, detach the old key (with empty ID) from the naming
			// registry. Without this, the subsequent PersistRecord call would fail because the path
			// is still registered under the old key, causing a path collision with the new key.
			u.Manifest().NamingRegistry().Detach(notificationState.NotificationManifest.Key())

			// Update manifest ID so it is persisted correctly after push
			notificationState.NotificationManifest.ID = created.ID

			notificationState.SetRemoteState(remoteNotification)
			u.changes.AddCreated(notificationState)
			return nil
		})
}
