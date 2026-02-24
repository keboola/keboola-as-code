package model

import (
	"fmt"
)

// NotificationManifest represents a notification manifest record.
type NotificationManifest struct {
	RecordState `json:"-"`
	NotificationKey
	Paths
	Relations Relations `json:"relations,omitempty" validate:"dive"`
}

// NewEmptyObject creates an empty Notification object with the key from this manifest.
func (n NotificationManifest) NewEmptyObject() Object {
	return &Notification{NotificationKey: n.NotificationKey}
}

// NewObjectState creates a new NotificationState wrapping this manifest.
func (n *NotificationManifest) NewObjectState() ObjectState {
	return &NotificationState{NotificationManifest: n}
}

// SortKey returns the sort key for this notification based on the sort mode.
func (n NotificationManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("%02d_notification_%s", n.Level(), n.Path())
	}
	return n.String()
}

// GetRelations returns the relations for this notification.
func (n *NotificationManifest) GetRelations() Relations {
	return n.Relations
}

// SetRelations sets the relations for this notification.
func (n *NotificationManifest) SetRelations(relations Relations) {
	n.Relations = relations
}

// AddRelation adds a relation to this notification.
func (n *NotificationManifest) AddRelation(relation Relation) {
	n.Relations = append(n.Relations, relation)
}
