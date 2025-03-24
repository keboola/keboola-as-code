package model

import (
	"github.com/sasha-s/go-deadlock"
)

type ChangesReplaceFunc func(ObjectState) ObjectState

// LocalChanges contains all processed objects in the local.UnitOfWork.
type LocalChanges struct {
	lock      *deadlock.Mutex
	loaded    []ObjectState    // list of the objects loaded from the filesystem
	persisted []ObjectState    // list of the new objects found in the filesystem
	created   []ObjectState    // list of the created objects (did not exist before)
	updated   []ObjectState    // list of the updated objects (existed before)
	saved     []ObjectState    // created + updated
	renamed   []RenameAction   // list of the renamed objects
	deleted   []ObjectManifest // list of the deleted objects
}

// RemoteChanges contains all processed objects in the remote.UnitOfWork.
type RemoteChanges struct {
	lock    *deadlock.Mutex
	loaded  []ObjectState // list of the objects loaded from the Storage API
	created []ObjectState // list of the created objects (did not exist before)
	updated []ObjectState // list of the updated objects (existed before)
	saved   []ObjectState // created + updated
	deleted []ObjectState // list of the deleted objects
}

func NewLocalChanges() *LocalChanges {
	return &LocalChanges{lock: &deadlock.Mutex{}}
}

func NewRemoteChanges() *RemoteChanges {
	return &RemoteChanges{lock: &deadlock.Mutex{}}
}

// Empty returns true if there are no changes.
func (c *LocalChanges) Empty() bool {
	return len(c.created) == 0 && len(c.loaded) == 0 && len(c.saved) == 0 && len(c.deleted) == 0 && len(c.renamed) == 0
}

// Loaded returns all objects loaded from the filesystem.
func (c *LocalChanges) Loaded() []ObjectState {
	return c.loaded
}

// Persisted returns all objects persisted from the filesystem.
func (c *LocalChanges) Persisted() []ObjectState {
	return c.persisted
}

// Created returns all objects saved to the filesystem, if they did not exist before.
func (c *LocalChanges) Created() []ObjectState {
	return c.created
}

// Updated returns all objects saved to the filesystem, if they exist before.
func (c *LocalChanges) Updated() []ObjectState {
	return c.updated
}

// Saved returns all objects saved to the filesystem.
func (c *LocalChanges) Saved() []ObjectState {
	return c.saved
}

// Renamed returns all renamed objects in the filesystem.
func (c *LocalChanges) Renamed() []RenameAction {
	return c.renamed
}

// Deleted returns all deleted objects from the filesystem.
func (c *LocalChanges) Deleted() []ObjectManifest {
	return c.deleted
}

func (c *LocalChanges) AddLoaded(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.loaded = append(c.loaded, objectState...)
}

func (c *LocalChanges) AddPersisted(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.persisted = append(c.persisted, objectState...)
}

func (c *LocalChanges) AddCreated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.created = append(c.created, objectState...)
	c.saved = append(c.saved, objectState...)
}

func (c *LocalChanges) AddUpdated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.updated = append(c.updated, objectState...)
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

func (c *LocalChanges) Replace(callback ChangesReplaceFunc) {
	c.loaded = replaceObjects(c.loaded, callback)
	c.persisted = replaceObjects(c.persisted, callback)
	c.created = replaceObjects(c.created, callback)
	c.updated = replaceObjects(c.updated, callback)
	c.saved = replaceObjects(c.saved, callback)
}

// Empty returns true if there are no changes.
func (c *RemoteChanges) Empty() bool {
	return len(c.created) == 0 && len(c.loaded) == 0 && len(c.saved) == 0 && len(c.deleted) == 0
}

// Loaded returns all objects loaded from the Storage API.
func (c *RemoteChanges) Loaded() []ObjectState {
	return c.loaded
}

// Created returns all saved objects to the Storage API, if they did not exist before.
func (c *RemoteChanges) Created() []ObjectState {
	return c.created
}

// Updated returns all saved objects to the Storage API, if they exist before.
func (c *RemoteChanges) Updated() []ObjectState {
	return c.updated
}

// Saved returns all saved objects to the Storage API.
func (c *RemoteChanges) Saved() []ObjectState {
	return c.saved
}

// Deleted returns all deleted objects from the Storage API.
func (c *RemoteChanges) Deleted() []ObjectState {
	return c.deleted
}

func (c *RemoteChanges) AddCreated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.created = append(c.created, objectState...)
	c.saved = append(c.saved, objectState...)
}

func (c *RemoteChanges) AddUpdated(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.updated = append(c.updated, objectState...)
	c.saved = append(c.saved, objectState...)
}

func (c *RemoteChanges) AddLoaded(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.loaded = append(c.loaded, objectState...)
}

func (c *RemoteChanges) AddDeleted(objectState ...ObjectState) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleted = append(c.deleted, objectState...)
}

func (c *RemoteChanges) Replace(callback ChangesReplaceFunc) {
	c.loaded = replaceObjects(c.loaded, callback)
	c.created = replaceObjects(c.created, callback)
	c.updated = replaceObjects(c.updated, callback)
	c.saved = replaceObjects(c.saved, callback)
	c.deleted = replaceObjects(c.deleted, callback)
}

// replaceObjects replaces value by callback, nil value is ignored.
func replaceObjects(in []ObjectState, callback ChangesReplaceFunc) []ObjectState {
	var out []ObjectState
	for _, oldValue := range in {
		// Skip nil
		if newValue := callback(oldValue); newValue != nil {
			out = append(out, newValue)
		}
	}
	return out
}
