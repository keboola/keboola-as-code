package model

import "sync"

// LocalChanges contains all processed objects in local.UnitOfWork.
type LocalChanges struct {
	lock      *sync.Mutex
	created   []ObjectState
	persisted []ObjectState
	loaded    []ObjectState
	saved     []ObjectState
	deleted   []ObjectManifest
	renamed   []RenameAction
}

// RemoteChanges contains all processed objects in remote.UnitOfWork.
type RemoteChanges struct {
	lock    *sync.Mutex
	created []ObjectState
	loaded  []ObjectState
	saved   []ObjectState
	deleted []ObjectState
}

func NewLocalChanges() *LocalChanges {
	return &LocalChanges{lock: &sync.Mutex{}}
}

func NewRemoteChanges() *RemoteChanges {
	return &RemoteChanges{lock: &sync.Mutex{}}
}

func (c *LocalChanges) Empty() bool {
	return len(c.created) == 0 && len(c.loaded) == 0 && len(c.saved) == 0 && len(c.deleted) == 0 && len(c.renamed) == 0
}

func (c *LocalChanges) Created() []ObjectState {
	return c.created
}

func (c *LocalChanges) Persisted() []ObjectState {
	return c.persisted
}

func (c *LocalChanges) Loaded() []ObjectState {
	return c.loaded
}

func (c *LocalChanges) Saved() []ObjectState {
	return c.saved
}

func (c *LocalChanges) Deleted() []ObjectManifest {
	return c.deleted
}

func (c *LocalChanges) Renamed() []RenameAction {
	return c.renamed
}

func (c *LocalChanges) AddCreated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.created = append(c.created, objectState...)
}

func (c *LocalChanges) AddPersisted(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.persisted = append(c.persisted, objectState...)
}

func (c *LocalChanges) AddLoaded(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.loaded = append(c.loaded, objectState...)
}

func (c *LocalChanges) AddSaved(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.saved = append(c.saved, objectState...)
}

func (c *LocalChanges) AddDeleted(objectManifest ...ObjectManifest) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleted = append(c.deleted, objectManifest...)
}

func (c *LocalChanges) AddRenamed(actions ...RenameAction) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.renamed = append(c.renamed, actions...)
}

func (c *RemoteChanges) Empty() bool {
	return len(c.created) == 0 && len(c.loaded) == 0 && len(c.saved) == 0 && len(c.deleted) == 0
}

func (c *RemoteChanges) Created() []ObjectState {
	return c.created
}

func (c *RemoteChanges) Loaded() []ObjectState {
	return c.loaded
}

func (c *RemoteChanges) Saved() []ObjectState {
	return c.saved
}

func (c *RemoteChanges) Deleted() []ObjectState {
	return c.deleted
}

func (c *RemoteChanges) AddCreated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.created = append(c.created, objectState...)
}

func (c *RemoteChanges) AddLoaded(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.loaded = append(c.loaded, objectState...)
}

func (c *RemoteChanges) AddSaved(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.saved = append(c.saved, objectState...)
}

func (c *RemoteChanges) AddDeleted(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleted = append(c.deleted, objectState...)
}
