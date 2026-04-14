package manifest

import (
	"sync"

	"github.com/keboola/go-utils/pkg/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Records contains model.ObjectManifest for each object: branch, config, row.
type Records struct {
	naming *naming.Registry
	sortBy string

	lock    *sync.Mutex
	all     *orderedmap.OrderedMap // common map for all Records: branches, configs and rows manifests
	loaded  bool
	changed bool
}

func NewRecords(sortBy string) *Records {
	return &Records{
		naming:  naming.NewRegistry(),
		sortBy:  sortBy,
		lock:    &sync.Mutex{},
		all:     orderedmap.New(),
		loaded:  true,
		changed: false,
	}
}

func (r *Records) SetRecords(records []model.ObjectManifest) error {
	r.loaded = false
	defer func() {
		// Track if manifest was changed after load
		r.loaded = true
		r.changed = false
	}()

	// Clear
	r.all = orderedmap.New()

	// Track records (only adds to r.all; parent-path resolution happens below).
	// Do not return early on error: successfully tracked records must still have
	// their parent paths resolved, otherwise they are left with parentPathSet=false
	// which causes panics during subsequent state loading.
	errs := errors.NewMultiError()
	for _, record := range records {
		if err := r.trackRecord(record); err != nil {
			errs.Append(err)
		}
	}

	// Resolve parent paths — possible only after all records are loaded.
	// Collect errors and delete failed records instead of aborting early.
	// This prevents records with an unresolved parent path (parentPathSet=false)
	// from remaining in the map and causing panics during subsequent state loading.
	//
	// Use a snapshot copy of the keys: orderedmap.Keys() returns the live internal
	// slice, and slices.Delete (Go 1.22+) zeroes freed tail elements, so iterating
	// over the live slice while deleting would silently skip later entries.
	for _, key := range append([]string{}, r.all.Keys()...) {
		v, found := r.all.Get(key)
		if !found {
			continue // unreachable today; guard against future cascading deletion
		}
		record := v.(model.ObjectManifest)
		if err := r.PersistRecord(record); err != nil {
			errs.Append(err)
			r.DeleteByKey(record.Key())
		}
	}

	// Cascading cleanup: when a child was persisted before its parent (possible
	// when parentage is determined by Relations.ParentKey), and the parent was
	// deleted above, the child remains with a stale path. Re-run until stable.
	for {
		removed := false
		for _, key := range append([]string{}, r.all.Keys()...) {
			v, found := r.all.Get(key)
			if !found {
				continue
			}
			record := v.(model.ObjectManifest)
			parentKey, err := record.ParentKey()
			if err != nil || parentKey == nil {
				continue // branch-level records or invalid relations — skip
			}
			if _, parentFound := r.GetRecord(parentKey); !parentFound {
				errs.Append(errors.Errorf("orphaned record %s: parent not found", record.Desc()))
				r.DeleteByKey(record.Key())
				removed = true
			}
		}
		if !removed {
			break
		}
	}

	return errs.ErrorOrNil()
}

func (r *Records) SortBy() string {
	return r.sortBy
}

func (r *Records) SetSortBy(sortBy string) {
	r.sortBy = sortBy
}

func (r *Records) All() []model.ObjectManifest {
	r.SortRecords()
	out := make([]model.ObjectManifest, len(r.all.Keys()))
	for i, k := range r.all.Keys() {
		v, _ := r.all.Get(k)
		out[i] = v.(model.ObjectManifest)
	}
	return out
}

func (r *Records) AllPersisted() []model.ObjectManifest {
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
func (r *Records) SortRecords() {
	r.all.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		aKey := a.Value.(model.ObjectManifest).SortKey(r.sortBy)
		bKey := b.Value.(model.ObjectManifest).SortKey(r.sortBy)
		return aKey < bKey
	})
}

func (r *Records) NamingRegistry() *naming.Registry {
	return r.naming
}

func (r *Records) IsChanged() bool {
	return r.changed
}

func (r *Records) ResetChanged() {
	r.changed = false
}

func (r *Records) MustGetRecord(key model.Key) model.ObjectManifest {
	if record, found := r.GetRecord(key); found {
		return record
	} else {
		panic(errors.Errorf("manifest record with key \"%s\"", key.String()))
	}
}

func (r *Records) GetRecord(key model.Key) (model.ObjectManifest, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r, found := r.all.Get(key.String()); found {
		return r.(model.ObjectManifest), found
	}
	return nil, false
}

func (r *Records) CreateOrGetRecord(key model.Key) (manifest model.ObjectManifest, found bool, err error) {
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
		panic(errors.Errorf("unexpected type \"%s\"", key))
	}

	if err := r.trackRecord(manifest); err != nil {
		return nil, false, err
	}

	return manifest, false, nil
}

func (r *Records) GetParent(manifest model.ObjectManifest) (model.ObjectManifest, error) {
	parentKey, err := manifest.ParentKey()
	if err != nil {
		return nil, err
	} else if parentKey == nil {
		return nil, nil
	}

	parent, found := r.GetRecord(parentKey)
	if !found {
		return nil, errors.Errorf(`manifest record for %s not found, referenced from %s`, parentKey.Desc(), manifest.Desc())
	}
	return parent, nil
}

// PersistRecord - store a new or existing record, and marks it as persisted.
func (r *Records) PersistRecord(record model.ObjectManifest) error {
	// Resolve parent path
	if !record.IsParentPathSet() {
		if err := r.ResolveParentPath(record); err != nil {
			return err
		}
	}

	// Attach record to the NamingRegistry
	if err := r.naming.Attach(record.Key(), record.GetAbsPath()); err != nil {
		return err
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// Mark persisted -> will be saved to manifest.json
	record.State().SetPersisted()

	r.all.Set(record.Key().String(), record)
	r.changed = true
	return nil
}

// trackRecord - store a new record and make sure it has unique key.
func (r *Records) trackRecord(record model.ObjectManifest) error {
	// Resolve parent path and attach record to the Naming
	if r.loaded {
		// All Records must be loaded to resolve parent path
		if err := r.ResolveParentPath(record); err != nil {
			return err
		}
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	// Make sure the key is unique
	if _, exists := r.all.Get(record.Key().String()); exists {
		return errors.Errorf(`duplicate %s in manifest`, record.Desc())
	}

	r.all.Set(record.Key().String(), record)
	return nil
}

func (r *Records) Delete(object model.WithKey) {
	r.DeleteByKey(object.Key())
}

func (r *Records) DeleteByKey(key model.Key) {
	record, found := r.GetRecord(key)
	if found {
		r.lock.Lock()
		defer r.lock.Unlock()
		record.State().SetDeleted()
		r.changed = r.changed || record.State().IsPersisted()
		r.all.Delete(key.String())
	}
}

func (r *Records) ResolveParentPath(record model.ObjectManifest) error {
	return r.doResolveParentPath(record, nil)
}

// doResolveParentPath recursive + fail on cyclic relations.
func (r *Records) doResolveParentPath(record, origin model.ObjectManifest) error {
	if origin != nil && record.Key().String() == origin.Key().String() {
		return errors.Errorf(`a cyclic relation was found when resolving path to %s`, origin.Desc())
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
