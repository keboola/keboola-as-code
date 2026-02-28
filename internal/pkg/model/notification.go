package model

import (
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

// Notification represents a notification subscription in the Keboola project.
type Notification struct {
	NotificationKey
	Event     keboola.NotificationEvent       `json:"event" validate:"required" diff:"true"`
	Filters   []keboola.NotificationFilter    `json:"filters,omitempty" diff:"true"`
	Recipient keboola.NotificationRecipient   `json:"recipient" validate:"required" diff:"true"`
	ExpiresAt *keboola.NotificationExpiration `json:"expiresAt,omitempty" diff:"true"`
	CreatedAt *time.Time                      `json:"createdAt,omitempty"`
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
