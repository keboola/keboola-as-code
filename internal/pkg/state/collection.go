package state

import (
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

type sorter = ObjectsSorter

// Collection implements model.Objects interface.
type Collection struct {
	sorter
	lock    *sync.Mutex
	objects *orderedmap.OrderedMap
}

func NewCollection(sorter ObjectsSorter) Objects {
	return &Collection{
		sorter:  sorter,
		lock:    &sync.Mutex{},
		objects: orderedmap.New(),
	}
}

// Add object to the collection.
// Error is returned if the object is already present.
func (c *Collection) Add(objects ...Object) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	errs := errors.NewMultiError()
	for _, object := range objects {
		key := object.Key()
		if c.has(key) {
			errs.Append(fmt.Errorf(`%s already exists`, key.String()))
		} else if err := c.add(object); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

// AddOrReplace object in the collection.
func (c *Collection) AddOrReplace(objects ...Object) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	errs := errors.NewMultiError()
	for _, object := range objects {
		if err := c.add(object); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

func (c *Collection) MustAdd(objects ...Object) {
	if err := c.Add(objects...); err != nil {
		panic(err)
	}
}

// Remove object from the collection.
func (c *Collection) Remove(keys ...Key) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Convert keys to a map.
	toRemove := make(map[Key]bool)
	for _, key := range keys {
		toRemove[key] = true
	}

	// Check all objects
	for _, object := range c.all() {
		key := object.Key()

		// Also remove the children.
		// If the parent is removed, this object will also be removed.
		parentKey, _ := object.ParentKey()
		if parentKey != nil && toRemove[parentKey] {
			toRemove[key] = true
		}

		// Remove object
		if toRemove[key] {
			c.objects.Delete(key.String())
		}
	}
}

// Get object from the collection.
func (c *Collection) Get(key Key) (Object, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if v, ok := c.objects.Get(key.String()); ok {
		return v.(Object), true
	}
	return nil, false
}

// GetOrNil object from the collection or returns nil if it is not present.
func (c *Collection) GetOrNil(key Key) Object {
	v, _ := c.Get(key)
	return v
}

// GetWithChildren gets object with all its children in the tree structure.
func (c *Collection) GetWithChildren(rootKey Key) (*ObjectWithChildren, bool) {
	rootObject, found := c.Get(rootKey)
	if !found {
		return nil, false
	}

	// Temporary map: parent -> children
	recordByKey := map[Key]*ObjectWithChildren{
		rootKey: {Object: rootObject, Children: make(map[Kind][]*ObjectWithChildren)},
	}

	// Generate children tree structure in one iteration
	for _, object := range c.All() {
		// Get parent key
		parentKey, err := object.ParentKey()
		if err != nil {
			// error is not expected, it is checked on Add operation
			panic(err)
		} else if parentKey == nil {
			// no parent
			continue
		}

		// Add object to the tree
		if parent, ok := recordByKey[parentKey]; ok {
			record := &ObjectWithChildren{Object: object, Children: make(map[Kind][]*ObjectWithChildren)}
			recordByKey[object.Key()] = record
			parent.Children[object.Kind()] = append(parent.Children[object.Kind()], record)
		}
	}

	return recordByKey[rootKey], true
}

// MustGet object from the collection otherwise panic occurs.
func (c *Collection) MustGet(key Key) Object {
	state, found := c.Get(key)
	if !found {
		panic(fmt.Errorf(`%s not found`, key.String()))
	}
	return state
}

// All gets all objects from the collection.
// The result is sorted.
func (c *Collection) All() []Object {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.all()
}

// AllWithChildren gets all core objects from the collection with children.
// If Kind.IsCore() is true (so the object type is: branch, config or config row),
// then the object is present in the result at the root level.
// Otherwise, the object (transformation, orchestration, code, phase, ...)  is included under its parent.
func (c *Collection) AllWithChildren() []*ObjectWithChildren {
	// Temporary map: parent -> children
	var rootObjects []*ObjectWithChildren
	recordByKey := map[Key]*ObjectWithChildren{}

	// Generate children tree structure in one iteration
	for _, object := range c.All() {
		record := &ObjectWithChildren{Object: object, Children: make(map[Kind][]*ObjectWithChildren)}

		// Is core object?
		if object.Kind().IsCore() {
			// Add to the root objects
			recordByKey[object.Key()] = record
			rootObjects = append(rootObjects, record)
			continue
		}

		// Get parent key
		parentKey, err := object.ParentKey()
		if err != nil {
			// error is not expected, it is checked on Add operation
			panic(err)
		} else if parentKey == nil {
			// no parent
			continue
		}

		// Add object to the tree
		if parent, ok := recordByKey[parentKey]; ok {
			recordByKey[object.Key()] = record
			parent.Children[object.Kind()] = append(parent.Children[object.Kind()], record)
		}
	}

	return rootObjects
}

// Branches gets all branches from the collection.
// The result is sorted.
func (c *Collection) Branches() (branches []*Branch) {
	for _, object := range c.All() {
		if v, ok := object.(*Branch); ok {
			branches = append(branches, v)
		}
	}
	return branches
}

// Configs gets all configs from the collection.
// The result is sorted.
func (c *Collection) Configs() (configs []*Config) {
	for _, object := range c.All() {
		if v, ok := object.(*Config); ok {
			configs = append(configs, v)
		}
	}
	return configs
}

// ConfigsFrom gets all configs from the branch.
// The result is sorted.
func (c *Collection) ConfigsFrom(branch BranchKey) (configs []*Config) {
	for _, object := range c.All() {
		if v, ok := object.(*Config); ok {
			if v.BranchId != branch.BranchId {
				continue
			}
			configs = append(configs, v)
		}
	}
	return configs
}

// ConfigsWithRows gets all configs with rows.
// The result is sorted.
func (c *Collection) ConfigsWithRows() (configs []*ConfigWithRows) {
	for _, config := range c.Configs() {
		configs = append(configs, &ConfigWithRows{Config: config, Rows: c.ConfigRowsFrom(config.ConfigKey)})
	}
	return configs
}

// ConfigsWithRowsFrom gets all configs and rows from the branch.
// The result is sorted.
func (c *Collection) ConfigsWithRowsFrom(branch BranchKey) (configs []*ConfigWithRows) {
	for _, config := range c.ConfigsFrom(branch) {
		configs = append(configs, &ConfigWithRows{Config: config, Rows: c.ConfigRowsFrom(config.ConfigKey)})
	}
	return configs
}

// ConfigRows gets all config rows.
// The result is sorted.
func (c *Collection) ConfigRows() (rows []*ConfigRow) {
	for _, object := range c.All() {
		if v, ok := object.(*ConfigRow); ok {
			rows = append(rows, v)
		}
	}
	return rows
}

// ConfigRowsFrom gets all config rows from the branch.
// The result is sorted.
func (c *Collection) ConfigRowsFrom(config ConfigKey) (rows []*ConfigRow) {
	for _, object := range c.All() {
		if v, ok := object.(*ConfigRow); ok {
			if v.BranchId != config.BranchId || v.ComponentId != config.ComponentId || v.ConfigId != config.ConfigId {
				continue
			}
			rows = append(rows, v)
		}
	}
	return rows
}

// add object to the collection and check if parent is already present.
func (c *Collection) add(object Object) error {
	parentKey, err := object.ParentKey()
	if err != nil {
		return errors.PrefixError(fmt.Sprintf(`cannot get parent of %s`, object.String()), err)
	}

	if parentKey != nil && !c.has(parentKey) {
		return errors.PrefixError(
			fmt.Sprintf(`parent %s not found`, parentKey.String()),
			fmt.Errorf(`referenced from %s`, object.String()),
		)
	}

	c.objects.Set(object.Key().String(), object)
	return nil
}

// has check if object is present in the collection.
func (c *Collection) has(key Key) bool {
	_, found := c.objects.Get(key.String())
	return found
}

func (c *Collection) all() []Object {
	c.objects.Sort(func(i *orderedmap.Pair, j *orderedmap.Pair) bool {
		return c.Less(i.Value.(Object).Key(), j.Value.(Object).Key())
	})

	out := make([]Object, c.objects.Len())
	for i, key := range c.objects.Keys() {
		// Get value
		v, _ := c.objects.Get(key)
		object := v.(Object)
		out[i] = object
	}

	return out
}
