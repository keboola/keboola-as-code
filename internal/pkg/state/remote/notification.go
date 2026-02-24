package remote

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// loadNotifications loads notification subscriptions using manifest to determine parent config.
// The manifest tracks which config owns each notification, so we match by subscription ID.
func (u *UnitOfWork) loadNotifications(ctx context.Context) error {
	// Load all notification subscriptions from API
	subscriptions, err := u.keboolaProjectAPI.
		ListNotificationSubscriptionsRequest().
		Send(ctx)
	if err != nil {
		return err
	}

	// Create map for quick lookup
	subsByID := make(map[keboola.NotificationSubscriptionID]*keboola.NotificationSubscription)
	for _, sub := range *subscriptions {
		subsByID[sub.ID] = sub
	}

	// Iterate through manifest to find notifications
	for _, record := range u.Manifest().All() {
		configManifest, ok := record.(*model.ConfigManifest)
		if !ok {
			continue
		}

		// Check if this config has notifications in manifest
		for _, notificationManifest := range configManifest.Notifications {
			// Find matching subscription from API
			if apiSub, found := subsByID[notificationManifest.ID]; found {
				// Create notification with parent config info from manifest
				notification := model.NewNotification(apiSub)
				notification.BranchID = configManifest.BranchID
				notification.ComponentID = configManifest.ComponentID
				notification.ConfigID = configManifest.ID

				// Load into state
				if err := u.loadObject(notification); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// createNotificationRequest builds an API request to create a notification subscription.
func (u *UnitOfWork) createNotificationRequest(notification *model.Notification) *keboola.CreateNotificationSubscriptionRequestBuilder {
	req := u.keboolaProjectAPI.
		NewCreateNotificationSubscriptionRequest(
			notification.Event,
			notification.Recipient.Channel,
			notification.Recipient.Address,
		)

	if len(notification.Filters) > 0 {
		req = req.WithFilters(notification.Filters)
	}

	// TODO: Handle expiration - NotificationExpiration doesn't expose fields
	// Need SDK update to support passing expiration directly
	_ = notification.ExpiresAt

	// TODO: Add config reference once SDK supports it
	// req = req.WithConfig(notification.BranchID, notification.ComponentID, notification.ConfigID)

	return req
}

// deleteNotification deletes a notification subscription via API.
func (u *UnitOfWork) deleteNotification(key model.NotificationKey) error {
	return u.keboolaProjectAPI.
		DeleteNotificationSubscriptionRequest(keboola.NotificationSubscriptionKey{ID: key.ID}).
		SendOrErr(u.ctx)
}
