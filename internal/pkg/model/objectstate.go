package model

import (
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"
)

type StateType int

const (
	StateTypeLocal StateType = iota
	StateTypeRemote
)

type ObjectState interface {
	RecordPaths
	Key
	Key() Key
	ObjectName() string
	HasManifest() bool
	SetManifest(record ObjectManifest)
	Manifest() ObjectManifest
	HasState(stateType StateType) bool
	GetState(stateType StateType) Object
	HasLocalState() bool
	SetLocalState(object Object)
	LocalState() Object
	HasRemoteState() bool
	SetRemoteState(object Object)
	RemoteState() Object
	LocalOrRemoteState() Object
	RemoteOrLocalState() Object
}

type BranchState struct {
	*BranchManifest
	Remote *Branch `validate:"omitempty,dive"`
	Local  *Branch `validate:"omitempty,dive"`
}

type ConfigState struct {
	*ConfigManifest
	Remote *Config `validate:"omitempty,dive"`
	Local  *Config `validate:"omitempty,dive"`
}

type ConfigRowState struct {
	*ConfigRowManifest
	Remote *ConfigRow `validate:"omitempty,dive"`
	Local  *ConfigRow `validate:"omitempty,dive"`
}

// ToApiObjectKey ...
func (b *BranchState) ToApiObjectKey() any {
	return storageapi.BranchKey{ID: b.Id}
}

// ToApiObjectKey ...
func (c *ConfigState) ToApiObjectKey() any {
	return storageapi.ConfigKey{BranchID: c.BranchId, ComponentID: c.ComponentId, ID: c.Id}
}

// ToApiObjectKey ...
func (r *ConfigRowState) ToApiObjectKey() any {
	return storageapi.ConfigRowKey{BranchID: r.BranchId, ComponentID: r.ComponentId, ConfigID: r.ConfigId, ID: r.Id}
}

func (b *BranchState) HasState(stateType StateType) bool {
	switch stateType {
	case StateTypeLocal:
		return b.Local != nil
	case StateTypeRemote:
		return b.Remote != nil
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (c *ConfigState) HasState(stateType StateType) bool {
	switch stateType {
	case StateTypeLocal:
		return c.Local != nil
	case StateTypeRemote:
		return c.Remote != nil
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (r *ConfigRowState) HasState(stateType StateType) bool {
	switch stateType {
	case StateTypeLocal:
		return r.Local != nil
	case StateTypeRemote:
		return r.Remote != nil
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (b *BranchState) GetState(stateType StateType) Object {
	switch stateType {
	case StateTypeLocal:
		return b.Local
	case StateTypeRemote:
		return b.Remote
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (c *ConfigState) GetState(stateType StateType) Object {
	switch stateType {
	case StateTypeLocal:
		return c.Local
	case StateTypeRemote:
		return c.Remote
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (r *ConfigRowState) GetState(stateType StateType) Object {
	switch stateType {
	case StateTypeLocal:
		return r.Local
	case StateTypeRemote:
		return r.Remote
	default:
		panic(fmt.Errorf(`unexpected state type "%T"`, stateType))
	}
}

func (b *BranchState) HasLocalState() bool {
	return b.Local != nil
}

func (c *ConfigState) HasLocalState() bool {
	return c.Local != nil
}

func (r *ConfigRowState) HasLocalState() bool {
	return r.Local != nil
}

func (b *BranchState) SetLocalState(object Object) {
	if object == nil {
		b.Local = nil
	} else {
		b.Local = object.(*Branch)
	}
}

func (c *ConfigState) SetLocalState(object Object) {
	if object == nil {
		c.Local = nil
	} else {
		c.Local = object.(*Config)
	}
}

func (r *ConfigRowState) SetLocalState(object Object) {
	if object == nil {
		r.Local = nil
	} else {
		r.Local = object.(*ConfigRow)
	}
}

func (b *BranchState) LocalState() Object {
	return b.Local
}

func (c *ConfigState) LocalState() Object {
	return c.Local
}

func (r *ConfigRowState) LocalState() Object {
	return r.Local
}

func (b *BranchState) HasRemoteState() bool {
	return b.Remote != nil
}

func (c *ConfigState) HasRemoteState() bool {
	return c.Remote != nil
}

func (r *ConfigRowState) HasRemoteState() bool {
	return r.Remote != nil
}

func (b *BranchState) SetRemoteState(object Object) {
	if object == nil {
		b.Remote = nil
	} else {
		b.Remote = object.(*Branch)
	}
}

func (c *ConfigState) SetRemoteState(object Object) {
	if object == nil {
		c.Remote = nil
	} else {
		c.Remote = object.(*Config)
	}
}

func (r *ConfigRowState) SetRemoteState(object Object) {
	if object == nil {
		r.Remote = nil
	} else {
		r.Remote = object.(*ConfigRow)
	}
}

func (b *BranchState) RemoteState() Object {
	return b.Remote
}

func (c *ConfigState) RemoteState() Object {
	return c.Remote
}

func (r *ConfigRowState) RemoteState() Object {
	return r.Remote
}

func (b *BranchState) LocalOrRemoteState() Object {
	switch {
	case b.HasLocalState():
		return b.LocalState()
	case b.HasRemoteState():
		return b.RemoteState()
	default:
		panic(fmt.Errorf("object Local or Remote state must be set"))
	}
}

func (c *ConfigState) LocalOrRemoteState() Object {
	switch {
	case c.HasLocalState():
		return c.LocalState()
	case c.HasRemoteState():
		return c.RemoteState()
	default:
		panic(fmt.Errorf("object Local or Remote state must be set"))
	}
}

func (r *ConfigRowState) LocalOrRemoteState() Object {
	switch {
	case r.HasLocalState():
		return r.LocalState()
	case r.HasRemoteState():
		return r.RemoteState()
	default:
		panic(fmt.Errorf("object Local or Remote state must be set"))
	}
}

func (b *BranchState) RemoteOrLocalState() Object {
	switch {
	case b.HasRemoteState():
		return b.RemoteState()
	case b.HasLocalState():
		return b.LocalState()
	default:
		panic(fmt.Errorf("object Remote or Local state must be set"))
	}
}

func (c *ConfigState) RemoteOrLocalState() Object {
	switch {
	case c.HasRemoteState():
		return c.RemoteState()
	case c.HasLocalState():
		return c.LocalState()
	default:
		panic(fmt.Errorf("object Remote or Local state must be set"))
	}
}

func (r *ConfigRowState) RemoteOrLocalState() Object {
	switch {
	case r.HasRemoteState():
		return r.RemoteState()
	case r.HasLocalState():
		return r.LocalState()
	default:
		panic(fmt.Errorf("object Remote or Local state must be set"))
	}
}

func (b *BranchState) HasManifest() bool {
	return b.BranchManifest != nil
}

func (c *ConfigState) HasManifest() bool {
	return c.ConfigManifest != nil
}

func (r *ConfigRowState) HasManifest() bool {
	return r.ConfigRowManifest != nil
}

func (b *BranchState) SetManifest(record ObjectManifest) {
	b.BranchManifest = record.(*BranchManifest)
}

func (c *ConfigState) SetManifest(record ObjectManifest) {
	c.ConfigManifest = record.(*ConfigManifest)
}

func (r *ConfigRowState) SetManifest(record ObjectManifest) {
	r.ConfigRowManifest = record.(*ConfigRowManifest)
}

func (b *BranchState) Manifest() ObjectManifest {
	return b.BranchManifest
}

func (c *ConfigState) Manifest() ObjectManifest {
	return c.ConfigManifest
}

func (r *ConfigRowState) Manifest() ObjectManifest {
	return r.ConfigRowManifest
}

func (b *BranchState) ObjectName() string {
	if b.Remote != nil {
		return b.Remote.Name
	}
	if b.Local != nil {
		return b.Local.Name
	}
	return ""
}

func (c *ConfigState) ObjectName() string {
	if c.Remote != nil {
		return c.Remote.Name
	}
	if c.Local != nil {
		return c.Local.Name
	}
	return ""
}

func (r *ConfigRowState) ObjectName() string {
	if r.Remote != nil {
		return r.Remote.Name
	}
	if r.Local != nil {
		return r.Local.Name
	}
	return ""
}
