package remote

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// loadNotifications loads notification subscriptions for configs.
// NOTE: Currently limited by SDK - NotificationSubscription doesn't include
// BranchID, ComponentID, or ConfigID in the API response.
// This will be enabled once the SDK is updated to include parent config info.
func (u *UnitOfWork) loadNotifications(ctx context.Context, configKey model.ConfigKey) error {
	subscriptions, err := u.keboolaProjectAPI.
		ListNotificationSubscriptionsRequest().
		Send(ctx)
	if err != nil {
		return err
	}

	// TODO: Once SDK includes config reference in subscription response:
	// for _, apiSub := range *subscriptions {
	//     if apiSub.ConfigID == configKey.ID {
	//         notification := model.NewNotification(apiSub)
	//         notification.BranchID = configKey.BranchID
	//         notification.ComponentID = configKey.ComponentID
	//         notification.ConfigID = configKey.ID
	//         if err := u.loadObject(notification); err != nil {
	//             return err
	//         }
	//     }
	// }

	_ = subscriptions
	return nil
}

// createNotification creates a new notification subscription via API.
func (u *UnitOfWork) createNotification(notification *model.Notification) error {
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

	_, err := req.Send(u.ctx)
	return err
}

// deleteNotification deletes a notification subscription via API.
func (u *UnitOfWork) deleteNotification(key model.NotificationKey) error {
	return u.keboolaProjectAPI.
		DeleteNotificationSubscriptionRequest(keboola.NotificationSubscriptionKey{ID: key.ID}).
		SendOrErr(u.ctx)
}
