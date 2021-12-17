package manifest

import (
	"fmt"
	"sync"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// records contains model.ObjectManifest for each object: branch, config, row.
type records struct {
	naming naming
	sortBy string

	lock    *sync.Mutex
	all     *orderedmap.OrderedMap // common map for all records: branches, configs and rows manifests
	loaded  bool
	changed bool
}

type naming interface {
	Attach(key model.Key, path model.PathInProject)
}

func newRecords(naming naming, sortBy string) *records {
	return &records{
		naming:  naming,
		sortBy:  sortBy,
		lock:    &sync.Mutex{},
		all:     orderedmap.New(),
		loaded:  true,
		changed: false,
	}
}

func (r *records) LoadFromContent(content *Content) error {
	// Read all manifest records
	r.loaded = false
	for _, branch := range content.Branches {
		if err := r.trackRecord(branch); err != nil {
			return err
		}
	}
	for _, config := range content.Configs {
		if err := r.trackRecord(config.ConfigManifest); err != nil {
			return err
		}
		for _, row := range config.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			if err := r.trackRecord(row); err != nil {
				return err
			}
		}
	}

	// Validate
	if err := r.validate(); err != nil {
		return err
	}

	// Connect records together and resolve parent path
	for _, key := range r.all.Keys() {
		record, _ := r.all.Get(key)
		if err := r.PersistRecord(record.(model.ObjectManifest)); err != nil {
			return err
		}
	}

	// Track if manifest was changed after load
	r.loaded = true
	r.changed = false
	return nil
}

func (r *records) All() []model.ObjectManifest {
	r.SortRecords()
	out := make([]model.ObjectManifest, len(r.all.Keys()))
	for i, k := range r.all.Keys() {
		v, _ := r.all.Get(k)
		out[i] = v.(model.ObjectManifest)
	}
	return out
}

func (r *records) AllPersisted() []model.ObjectManifest {
	r.SortRecords()
	var out []model.ObjectManifest
	for _, k := range r.all.Keys() {
		vRaw, _ := r.all.Get(k)
		v := vRaw.(model.ObjectManifest)
		if v.State().IsPersisted() {
			out = append(out, v)
		}
	}
	return out
}

// SortRecords in manifest + ensure order of processing: branch, config, configRow.
func (r *records) SortRecords() {
	r.all.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value.(model.ObjectManifest).SortKey(r.sortBy)
		bKey := b.Value.(model.ObjectManifest).SortKey(r.sortBy)
		return aKey < bKey
	})
}

func (r *records) IsChanged() bool {
	return r.changed
}

func (r *records) ResetChanged() {
	r.changed = false
}

func (r *records) MustGetRecord(key model.Key) model.ObjectManifest {
	if record, found := r.GetRecord(key); found {
		return record
	} else {
		panic(fmt.Errorf("manifest record with key \"%s\"", key.String()))
	}
}

func (r *records) GetRecord(key model.Key) (model.ObjectManifest, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r, found := r.all.Get(key.String()); found {
		return r.(model.ObjectManifest), found
	}
	return nil, false
}

func (r *records) CreateOrGetRecord(key model.Key) (manifest model.ObjectManifest, found bool, err error) {
	// Get
	manifest, found = r.GetRecord(key)
	if found {
		return manifest, found, nil
	}

	// Create
	switch v := key.(type) {
	case model.BranchKey:
		manifest = &model.BranchManifest{BranchKey: v}
	case model.ConfigKey:
		manifest = &model.ConfigManifest{ConfigKey: v}
	case model.ConfigRowKey:
		manifest = &model.ConfigRowManifest{ConfigRowKey: v}
	default:
		panic(fmt.Errorf("unexpected type \"%s\"", key))
	}

	if err := r.trackRecord(manifest); err != nil {
		return nil, false, err
	}

	return manifest, false, nil
}

func (r *records) GetParent(manifest model.ObjectManifest) (model.ObjectManifest, error) {
	parentKey, err := manifest.ParentKey()
	if err != nil {
		return nil, err
	} else if parentKey == nil {
		return nil, nil
	}

	parent, found := r.GetRecord(parentKey)
	if !found {
		return nil, fmt.Errorf(`manifest record for %s not found, referenced from %s`, parentKey.Desc(), manifest.Desc())
	}
	return parent, nil
}

// PersistRecord - store a new or existing record, and marks it as persisted.
func (r *records) PersistRecord(record model.ObjectManifest) error {
	// Resolve parent path
	if !record.IsParentPathSet() {
		if err := r.ResolveParentPath(record); err != nil {
			return err
		}
	}

	// Attach record to the naming
	r.naming.Attach(record.Key(), record.GetPathInProject())

	r.lock.Lock()
	defer r.lock.Unlock()

	// Mark persisted -> will be saved to manifest.json
	record.State().SetPersisted()

	r.all.Set(record.Key().String(), record)
	r.changed = true
	return nil
}

// trackRecord - store a new record and make sure it has unique key.
func (r *records) trackRecord(record model.ObjectManifest) error {
	// Resolve parent path and attach record to the naming
	if r.loaded {
		// All records must be loaded to resolve parent path
		if err := r.ResolveParentPath(record); err != nil {
			return err
		}
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// Make sure the key is unique
	if _, exists := r.all.Get(record.Key().String()); exists {
		return fmt.Errorf(`duplicate %s in manifest`, record.Desc())
	}

	r.all.Set(record.Key().String(), record)
	return nil
}

func (r *records) Delete(object model.WithKey) {
	r.DeleteByKey(object.Key())
}

func (r *records) DeleteByKey(key model.Key) {
	record, found := r.GetRecord(key)
	if found {
		r.lock.Lock()
		defer r.lock.Unlock()
		record.State().SetDeleted()
		r.changed = r.changed || record.State().IsPersisted()
		r.all.Delete(key.String())
	}
}

func (r *records) ResolveParentPath(record model.ObjectManifest) error {
	return r.doResolveParentPath(record, nil)
}

// doResolveParentPath recursive + fail on cyclic relations.
func (r *records) doResolveParentPath(record, origin model.ObjectManifest) error {
	if origin != nil && record.Key().String() == origin.Key().String() {
		return fmt.Errorf(`a cyclic relation was found when resolving path to %s`, origin.Desc())
	}

	if origin == nil {
		origin = record
	}

	parent, err := r.GetParent(record)
	switch {
	case err != nil:
		return err
	case parent != nil:
		// Recursively resolve the parent path, if it is not set
		if !parent.IsParentPathSet() {
			if err := r.doResolveParentPath(parent, origin); err != nil {
				return err
			}
		}
		record.SetParentPath(parent.Path())
	default:
		// branch - no parent
		record.SetParentPath("")
	}

	return nil
}

func (r *records) validate() error {
	if err := validator.Validate(r.All()); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}
