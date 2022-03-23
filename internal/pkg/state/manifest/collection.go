package manifest

import (
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

// Collection of model.ObjectManifest for each object: branch, config, row.
type Collection struct {
	sorter model.ObjectsSorter
	naming *naming.Registry

	lock    *sync.Mutex
	records *orderedmap.OrderedMap
	changed bool
}

func NewCollection(naming *naming.Registry, sorter model.ObjectsSorter) *Collection {
	return &Collection{
		naming:  naming,
		sorter:  sorter,
		lock:    &sync.Mutex{},
		records: orderedmap.New(),
		changed: false,
	}
}

func (r *Collection) IsChanged() bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.changed
}

func (r *Collection) ResetChanged() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.resetChanged()
}

func (r *Collection) Sorter() model.ObjectsSorter {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.sorter
}

func (r *Collection) SetSorter(sorter model.ObjectsSorter) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.sorter = sorter
}

func (r *Collection) NamingRegistry() *naming.Registry {
	return r.naming
}

func (r *Collection) All() []model.ObjectManifest {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.all()
}

func (r *Collection) Get(key model.Key) (model.ObjectManifest, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.get(key)
}

func (r *Collection) Set(records ...model.ObjectManifest) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	defer r.resetChanged()

	// Clear
	r.records = orderedmap.New()
	r.naming.Clear()

	// Add records
	return r.add(records...)
}

func (r *Collection) Add(records ...model.ObjectManifest) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.add(records...)
}

func (r *Collection) Remove(keys ...model.Key) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Convert keys to a map.
	toRemove := make(map[model.Key]bool)
	for _, key := range keys {
		toRemove[key] = true
	}

	// Check all objects
	for _, objectManifest := range r.all() {
		// Also remove the children.
		// If the parent is removed, this object will also be removed.
		key := objectManifest.Key()
		parentKey, _ := objectManifest.ParentKey()
		if parentKey != nil && toRemove[parentKey] {
			toRemove[key] = true
		}

		// Remove record
		if toRemove[key] {
			r.changed = true
			r.naming.Detach(key)
			r.records.Delete(key.String())
		}
	}
}

func (r *Collection) all() []model.ObjectManifest {
	// Sort
	r.records.Sort(func(i *orderedmap.Pair, j *orderedmap.Pair) bool {
		iKey := i.Value.(model.ObjectManifest).Key()
		jKey := j.Value.(model.ObjectManifest).Key()
		return r.sorter.Less(iKey, jKey)
	})

	// Convert to slice
	out := make([]model.ObjectManifest, len(r.records.Keys()))
	for i, k := range r.records.Keys() {
		v, _ := r.records.Get(k)
		out[i] = v.(model.ObjectManifest)
	}
	return out
}

func (r *Collection) get(key model.Key) (model.ObjectManifest, bool) {
	if v, found := r.records.Get(key.String()); found {
		return v.(model.ObjectManifest), found
	}
	return nil, false
}

func (r *Collection) add(records ...model.ObjectManifest) error {
	errors := utils.NewMultiError()
	for _, objectManifest := range records {
		// Get parent
		parentKey, err := objectManifest.ParentKey()
		if err != nil {
			errors.Append(utils.PrefixError(fmt.Sprintf(`cannot get parent of %s`, objectManifest.String()), err))
			continue
		}

		// Check if parent exists
		if parentKey != nil {
			if _, found := r.get(parentKey); !found {
				errors.Append(utils.PrefixError(
					fmt.Sprintf(`parent %s not found`, parentKey.String()),
					fmt.Errorf(`referenced from %s`, objectManifest.String()),
				))
				continue
			}
		}

		// Attach path to the naming
		if err := r.naming.Attach(objectManifest.Key(), objectManifest.GetAbsPath()); err != nil {
			errors.Append(err)
			continue
		}

		// Add record
		r.records.Set(objectManifest.Key().String(), objectManifest)
		r.changed = true
	}

	return errors.ErrorOrNil()
}

func (r *Collection) resetChanged() {
	r.changed = false
}
