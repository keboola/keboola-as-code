package manifest

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// Collection of model.ObjectManifest for each object: branch, config, row.
type Collection struct {
	ctx    context.Context
	sorter model.ObjectsSorter
	naming *naming.Registry

	lock    *sync.Mutex
	records *orderedmap.OrderedMap
	changed bool
}

// parentPathResolver is a helper struct to resolve record parent path.
type parentPathResolver struct {
	collection    *Collection
	newRecords    []model.ObjectManifest
	newRecordsMap map[model.Key]model.ObjectManifest
	processing    map[model.Key]bool
	processed     map[model.Key]error
}

func NewCollection(ctx context.Context, naming *naming.Registry, sorter model.ObjectsSorter) *Collection {
	return &Collection{
		ctx:     ctx,
		naming:  naming,
		sorter:  sorter,
		lock:    &sync.Mutex{},
		records: orderedmap.New(),
		changed: false,
	}
}

func (c *Collection) IsChanged() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.changed
}

func (c *Collection) ResetChanged() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.resetChanged()
}

func (c *Collection) Sorter() model.ObjectsSorter {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.sorter
}

func (c *Collection) SetSorter(sorter model.ObjectsSorter) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.sorter = sorter
}

func (c *Collection) NamingRegistry() *naming.Registry {
	return c.naming
}

func (c *Collection) All() []model.ObjectManifest {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.all()
}

func (c *Collection) Get(key model.Key) (model.ObjectManifest, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.get(key)
}

func (c *Collection) Set(records []model.ObjectManifest) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	defer c.resetChanged()

	// Clear
	c.records = orderedmap.New()
	c.naming.Clear()

	// Add records
	return c.add(records...)
}

func (c *Collection) Add(records ...model.ObjectManifest) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.add(records...)
}

func (c *Collection) MustAdd(records ...model.ObjectManifest) {
	if err := c.Add(records...); err != nil {
		panic(err)
	}
}

func (c *Collection) Remove(keys ...model.Key) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// Convert keys to a map.
	toRemove := make(map[model.Key]bool)
	for _, key := range keys {
		toRemove[key] = true
	}

	// Check all objects
	for _, objectManifest := range c.all() {
		// Also remove the children.
		// If the parent is removed, this object will also be removed.
		key := objectManifest.Key()
		parentKey, _ := objectManifest.ParentKey()
		if parentKey != nil && toRemove[parentKey] {
			toRemove[key] = true
		}

		// Remove record
		if toRemove[key] {
			c.changed = true
			c.naming.Detach(key)
			c.records.Delete(key.String())
		}
	}
}

func (c *Collection) all() []model.ObjectManifest {
	// Sort
	c.records.Sort(func(i *orderedmap.Pair, j *orderedmap.Pair) bool {
		iKey := i.Value.(model.ObjectManifest).Key()
		jKey := j.Value.(model.ObjectManifest).Key()
		return c.sorter.Less(iKey, jKey)
	})

	// Convert to slice
	out := make([]model.ObjectManifest, len(c.records.Keys()))
	for i, k := range c.records.Keys() {
		v, _ := c.records.Get(k)
		out[i] = v.(model.ObjectManifest)
	}
	return out
}

func (c *Collection) get(key model.Key) (model.ObjectManifest, bool) {
	if v, found := c.records.Get(key.String()); found {
		return v.(model.ObjectManifest), found
	}
	return nil, false
}

func (c *Collection) add(records ...model.ObjectManifest) error {
	errs := errors.NewMultiError()
	records, err := c.resolveParentPaths(records)
	if err != nil {
		errs.Append(err)
	}

	// Add valid records
	for _, record := range records {
		if err := c.addOne(record); err != nil {
			errs.Append(errors.PrefixError(fmt.Sprintf(`invalid %s`, record.String()), err))
		}
	}

	return errs.ErrorOrNil()
}

func (c *Collection) addOne(record model.ObjectManifest) error {
	// Check path
	if record.Path().RelativePath() == "" {
		return fmt.Errorf("path is not set")
	}

	// Attach path to the naming
	if err := c.naming.Attach(record.Key(), record.Path()); err != nil {
		return err
	}

	// Validate record
	if err := validator.Validate(c.ctx, record); err != nil {
		return err
	}

	// Add record
	c.records.Set(record.Key().String(), record)
	c.changed = true
	return nil
}

func (c *Collection) resetChanged() {
	c.changed = false
}

// resolveParentPaths by parent key and detect cyclic relation.y
func (c *Collection) resolveParentPaths(newRecords []model.ObjectManifest) (validRecords []model.ObjectManifest, err error) {
	// Sort records
	sort.SliceStable(newRecords, func(i, j int) bool {
		return c.sorter.Less(newRecords[i].Key(), newRecords[j].Key())
	})

	// Create resolver
	r := &parentPathResolver{
		collection:    c,
		newRecords:    newRecords,
		newRecordsMap: make(map[model.Key]model.ObjectManifest),
		processing:    make(map[model.Key]bool),
		processed:     make(map[model.Key]error),
	}

	// Add new records to the map, for quick access
	for _, record := range newRecords {
		r.newRecordsMap[record.Key()] = record
	}

	// Process all
	errs := errors.NewMultiError()
	for _, record := range r.newRecords {
		if valid, err := r.process(record, nil); valid {
			validRecords = append(validRecords, record)
		} else if err != nil {
			errs.Append(err)
		}
	}
	return validRecords, errs.ErrorOrNil()
}

func (r *parentPathResolver) process(record model.ObjectManifest, path []model.Key) (valid bool, err error) {
	key := record.Key()

	// Is already processed?
	if err, found := r.processed[key]; found {
		// The error is already logged, so we're not returning it
		return err == nil, nil
	}

	// Cyclic relation?
	if r.processing[key] {
		details := errors.NewMultiError()
		for _, key := range path {
			details.Append(fmt.Errorf(`%s is child of`, key.String()))
		}
		details.Append(fmt.Errorf(key.String()))
		return false, errors.PrefixError("a cyclic relation found", details)
	}

	// Mark as processing
	r.processing[key] = true
	defer func() {
		// Reset processing state and store error if any
		r.processing[key] = false
		r.processed[key] = err
	}()

	// Get parent
	parentKey, err := record.ParentKey()
	if err != nil {
		return false, errors.PrefixError(fmt.Sprintf(`cannot get parent of %s`, record.String()), err)
	}

	// Top level object
	if parentKey == nil {
		p := record.Path().WithParentPath("")
		record.SetPath(p)
		return true, nil
	}
	// Get parent
	var parent model.ObjectManifest
	var found bool
	if parent, found = r.newRecordsMap[parentKey]; found {
		// Parent has been found in the new records
	} else if parent, found = r.collection.get(parentKey); found {
		// Parent has been found in the old records
	} else {
		return false, errors.PrefixError(
			fmt.Sprintf(`%s not found`, parentKey.String()),
			fmt.Errorf(`referenced as a parent of %s`, record.String()),
		)
	}

	// Process parent
	if valid, err = r.process(parent, append(path, key)); !valid {
		return false, err
	}

	// Set parent path
	p := record.Path().WithParentPath(parent.Path().String())
	record.SetPath(p)
	return true, nil
}
