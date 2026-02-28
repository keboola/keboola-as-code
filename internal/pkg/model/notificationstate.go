package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// NotificationState wraps notification manifest and local/remote states.
type NotificationState struct {
	*NotificationManifest
	Remote *Notification
	Local  *Notification
	Ignore bool
}

// ObjectName returns the subscription ID as the object name.
func (n *NotificationState) ObjectName() string {
	return string(n.ID)
}

// SetManifest sets the manifest for this notification state.
func (n *NotificationState) SetManifest(record ObjectManifest) {
	n.NotificationManifest = record.(*NotificationManifest)
}

// Manifest returns the manifest for this notification state.
func (n *NotificationState) Manifest() ObjectManifest {
	return n.NotificationManifest
}

// GetState returns the object for the specified state type.
func (n *NotificationState) GetState(stateType StateType) Object {
	switch stateType {
	case StateTypeLocal:
		return n.Local
	case StateTypeRemote:
		return n.Remote
	default:
		panic(errors.Errorf(`unexpected state type "%T"`, stateType))
	}
}

// HasState returns true if the notification has the specified state type.
func (n *NotificationState) HasState(stateType StateType) bool {
	switch stateType {
	case StateTypeLocal:
		return n.Local != nil
	case StateTypeRemote:
		return n.Remote != nil
	default:
		panic(errors.Errorf(`unexpected state type "%T"`, stateType))
	}
}

// HasManifest returns true if the notification has a manifest.
func (n *NotificationState) HasManifest() bool {
	return n.NotificationManifest != nil
}

// HasLocalState returns true if the notification has local state.
func (n *NotificationState) HasLocalState() bool {
	return n.Local != nil
}

// HasRemoteState returns true if the notification has remote state.
func (n *NotificationState) HasRemoteState() bool {
	return n.Remote != nil
}

// IsIgnored returns true if the notification is ignored.
func (n *NotificationState) IsIgnored() bool {
	return n.Ignore
}

// SetLocalState sets the local state for the notification.
func (n *NotificationState) SetLocalState(object Object) {
	if object == nil {
		n.Local = nil
	} else {
		n.Local = object.(*Notification)
	}
}

// SetRemoteState sets the remote state for the notification.
func (n *NotificationState) SetRemoteState(object Object) {
	if object == nil {
		n.Remote = nil
	} else {
		n.Remote = object.(*Notification)
	}
}

// LocalState returns the local notification object.
func (n *NotificationState) LocalState() Object {
	if n.Local == nil {
		return nil
	}
	return n.Local
}

// RemoteState returns the remote notification object.
func (n *NotificationState) RemoteState() Object {
	if n.Remote == nil {
		return nil
	}
	return n.Remote
}

// GetRelativePath returns the relative path to the notification directory.
func (n *NotificationState) GetRelativePath() string {
	return string(n.RelativePath)
}

// GetAbsPath returns the absolute path to the notification directory.
func (n *NotificationState) GetAbsPath() AbsPath {
	return n.AbsPath
}

// LocalOrRemoteState returns the local state if present, otherwise remote state.
func (n *NotificationState) LocalOrRemoteState() Object {
	switch {
	case n.HasLocalState():
		return n.LocalState()
	case n.HasRemoteState():
		return n.RemoteState()
	default:
		panic(errors.New("object Local or Remote state must be set"))
	}
}

// RemoteOrLocalState returns the remote state if present, otherwise local state.
func (n *NotificationState) RemoteOrLocalState() Object {
	switch {
	case n.HasRemoteState():
		return n.RemoteState()
	case n.HasLocalState():
		return n.LocalState()
	default:
		panic(errors.New("object Remote or Local state must be set"))
	}
}
