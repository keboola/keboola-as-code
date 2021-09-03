package model

import "fmt"

type ObjectState interface {
	Level() int       // hierarchical level, "1" for branch, "2" for config, ...
	Key() Key         // unique key for all objects
	ObjectId() string // eg. config id
	Kind() Kind       // branch, config, ...
	RelativePath() string
	HasManifest() bool
	SetManifest(record Record)
	Manifest() Record
	HasLocalState() bool
	SetLocalState(object Object)
	LocalState() Object
	HasRemoteState() bool
	SetRemoteState(object Object)
	RemoteState() Object
	LocalOrRemoteState() Object
}

type RecordProvider interface {
}

type BranchState struct {
	*BranchManifest
	Remote *Branch `validate:"omitempty,dive"`
	Local  *Branch `validate:"omitempty,dive"`
}

type ConfigState struct {
	*ConfigManifest
	Component *Component `validate:"dive"`
	Remote    *Config    `validate:"omitempty,dive"`
	Local     *Config    `validate:"omitempty,dive"`
}

type ConfigRowState struct {
	*ConfigRowManifest
	Remote *ConfigRow `validate:"omitempty,dive"`
	Local  *ConfigRow `validate:"omitempty,dive"`
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

func (b *BranchState) HasManifest() bool {
	return b.BranchManifest != nil
}

func (c *ConfigState) HasManifest() bool {
	return c.ConfigManifest != nil
}

func (r *ConfigRowState) HasManifest() bool {
	return r.ConfigRowManifest != nil
}

func (b *BranchState) SetManifest(record Record) {
	b.BranchManifest = record.(*BranchManifest)
}

func (c *ConfigState) SetManifest(record Record) {
	c.ConfigManifest = record.(*ConfigManifest)
}

func (r *ConfigRowState) SetManifest(record Record) {
	r.ConfigRowManifest = record.(*ConfigRowManifest)
}

func (b *BranchState) Manifest() Record {
	return b.BranchManifest
}

func (c *ConfigState) Manifest() Record {
	return c.ConfigManifest
}

func (r *ConfigRowState) Manifest() Record {
	return r.ConfigRowManifest
}

func (b *BranchState) GetName() string {
	if b.Remote != nil {
		return b.Remote.Name
	}
	if b.Local != nil {
		return b.Local.Name
	}
	return ""
}

func (c *ConfigState) GetName() string {
	if c.Remote != nil {
		return c.Remote.Name
	}
	if c.Local != nil {
		return c.Local.Name
	}
	return ""
}

func (r *ConfigRowState) GetName() string {
	if r.Remote != nil {
		return r.Remote.Name
	}
	if r.Local != nil {
		return r.Local.Name
	}
	return ""
}
