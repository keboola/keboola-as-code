package model

import "sync"

type ChangesReplaceFunc func(Object) Object

// Changes contains all processed objects in the remote.UnitOfWork.
type Changes struct {
	lock    *sync.Mutex
	loaded  []Object // list of the objects loaded from the backend
	created []Object // list of the created objects (did not exist before)
	updated []Object // list of the updated objects (existed before)
	saved   []Object // created + updated
	deleted []Key    // list of the deleted objects
}

func NewChanges() *Changes {
	return &Changes{lock: &sync.Mutex{}}
}

// Empty returns true if there are no changes.
func (c *Changes) Empty() bool {
	return len(c.created) == 0 && len(c.loaded) == 0 && len(c.saved) == 0 && len(c.deleted) == 0
}

// Loaded returns all objects loaded from the backend.
func (c *Changes) Loaded() []Object {
	return c.loaded
}

// Created returns all saved objects to the backend, if they did not exist before.
func (c *Changes) Created() []Object {
	return c.created
}

// Updated returns all saved objects to the backend, if they exist before.
func (c *Changes) Updated() []Object {
	return c.updated
}

// Saved returns all saved objects to the backend.
func (c *Changes) Saved() []Object {
	return c.saved
}

// Deleted returns all deleted keys from the backend.
func (c *Changes) Deleted() []Key {
	return c.deleted
}

func (c *Changes) AddCreated(objectState ...Object) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.created = append(c.created, objectState...)
	c.saved = append(c.saved, objectState...)
}

func (c *Changes) AddUpdated(objectState ...Object) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.updated = append(c.updated, objectState...)
	c.saved = append(c.saved, objectState...)
}

func (c *Changes) AddLoaded(objectState ...Object) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.loaded = append(c.loaded, objectState...)
}

func (c *Changes) AddDeleted(keys ...Key) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.deleted = append(c.deleted, keys...)
}

func (c *Changes) Replace(callback ChangesReplaceFunc) {
	c.loaded = replaceObjects(c.loaded, callback)
	c.created = replaceObjects(c.created, callback)
	c.updated = replaceObjects(c.updated, callback)
	c.saved = replaceObjects(c.saved, callback)
}

// replaceObjects replaces value by callback, nil value is ignored.
func replaceObjects(in []Object, callback ChangesReplaceFunc) []Object {
	var out []Object
	for _, oldValue := range in {
		// Skip nil
		if newValue := callback(oldValue); newValue != nil {
			out = append(out, newValue)
		}
	}
	return out
}
