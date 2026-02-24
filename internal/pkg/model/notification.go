package model

import (
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// Notification represents a notification subscription in the Keboola project.
type Notification struct {
	NotificationKey
	Event     keboola.NotificationEvent       `json:"event" validate:"required" diff:"true" metaFile:"true"`
	Filters   []keboola.NotificationFilter    `json:"filters,omitempty" diff:"true" metaFile:"true"`
	Recipient keboola.NotificationRecipient   `json:"recipient" validate:"required" diff:"true" metaFile:"true"`
	ExpiresAt *keboola.NotificationExpiration `json:"expiresAt,omitempty" diff:"true" metaFile:"true"`
	CreatedAt *time.Time                      `json:"createdAt,omitempty" metaFile:"true"`
	Relations Relations                       `json:"-" validate:"dive" diff:"true"`
}

// NewNotification creates a notification model from API values.
// Note: BranchID, ComponentID, and ConfigID must be set separately after creation
// as they are not returned by the notification API.
func NewNotification(apiValue *keboola.NotificationSubscription) *Notification {
	out := &Notification{}
	out.ID = apiValue.ID
	out.Event = apiValue.Event
	out.Filters = apiValue.Filters
	out.Recipient = apiValue.Recipient
	out.ExpiresAt = apiValue.ExpiresAt
	out.CreatedAt = apiValue.CreatedAt
	return out
}

// ToAPIObject converts notification to API subscription object.
// Note: This method exists for interface compatibility but notifications use
// specialized API methods for creation/deletion, not generic update operations.
func (n *Notification) ToAPIObject(_ string, changedFields ChangedFields) (*keboola.NotificationSubscription, []string) {
	out := &keboola.NotificationSubscription{}
	out.ID = n.ID
	out.Event = n.Event
	out.Filters = n.Filters
	out.Recipient = n.Recipient
	out.ExpiresAt = n.ExpiresAt
	out.CreatedAt = n.CreatedAt
	return out, changedFields.Slice()
}

// ToAPIObjectKey returns API key for this notification.
func (n *Notification) ToAPIObjectKey() any {
	return keboola.NotificationSubscriptionKey{ID: n.ID}
}

// ObjectName returns the string representation of the notification for display.
func (n *Notification) ObjectName() string {
	return string(n.ID)
}

// SetObjectID sets the notification subscription ID.
func (n *Notification) SetObjectID(objectID any) {
	n.ID = objectID.(keboola.NotificationSubscriptionID)
}

// GetRelations returns relations of the notification.
func (n *Notification) GetRelations() Relations {
	return n.Relations
}

// SetRelations sets relations for the notification.
func (n *Notification) SetRelations(relations Relations) {
	n.Relations = relations
}

// AddRelation adds a relation to the notification.
func (n *Notification) AddRelation(relation Relation) {
	n.Relations = append(n.Relations, relation)
}
