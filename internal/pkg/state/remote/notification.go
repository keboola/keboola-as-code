package remote

import (
	"context"
	"fmt"
	"time"

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

	// Build map of "branchID/configID" -> config manifest for collision-safe lookup
	configsByKey := make(map[string]*model.ConfigManifest)
	for _, record := range u.Manifest().All() {
		if configManifest, ok := record.(*model.ConfigManifest); ok {
			key := fmt.Sprintf("%d/%s", configManifest.BranchID, configManifest.ID)
			configsByKey[key] = configManifest
		}
	}

	// Load each notification from API
	for _, apiSub := range *subscriptions {
		// Collect branch.id and job.configuration.id from equality filters
		var branchIDStr, configIDStr string
		for _, filter := range apiSub.Filters {
			if filter.Operator != keboola.NotificationFilterOperatorEquals {
				continue
			}
			switch filter.Field {
			case "branch.id":
				branchIDStr = filter.Value
			case "job.configuration.id":
				configIDStr = filter.Value
			}
		}

		// Skip if no config ID filter present
		if configIDStr == "" {
			continue
		}

		// Look up config using composite key to avoid branch collisions
		var parentConfig *model.ConfigManifest
		if branchIDStr != "" {
			parentConfig = configsByKey[branchIDStr+"/"+configIDStr]
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
	autoFilters = append(autoFilters, notification.Filters...)

	if len(autoFilters) > 0 {
		req = req.WithFilters(autoFilters)
	}

	if notification.ExpiresAt != nil {
		expiresAtStr := notification.ExpiresAt.String()
		if t, err := time.Parse(time.RFC3339, expiresAtStr); err == nil {
			req = req.WithAbsoluteExpiration(t)
		} else {
			req = req.WithRelativeExpiration(expiresAtStr)
		}
	}

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
			var notifErr *keboola.NotificationError
			if errors.As(err, &notifErr) && notifErr.StatusCode() == 400 {
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
			u.Manifest().NamingRegistry().Detach(notificationState.Key())

			// Update manifest ID so it is persisted correctly after push
			notificationState.ID = created.ID

			notificationState.SetRemoteState(remoteNotification)
			u.changes.AddCreated(notificationState)
			return nil
		})
}
