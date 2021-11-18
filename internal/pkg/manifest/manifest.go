package manifest

import (
	"fmt"
	"sync"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "manifest.json"
)

type Manifest struct {
	fs       filesystem.Fs
	*Content `validate:"required,dive"` // content of the file, updated only on load/save
	loaded   bool
	changed  bool
	records  orderedmap.OrderedMap // common map for all: branches, configs and rows manifests
	lock     *sync.Mutex
}

type Content struct {
	Version int           `json:"version" validate:"required,min=1,max=1"`
	Project model.Project `json:"project" validate:"required"`
	SortBy  string        `json:"sortBy" validate:"oneof=id path"`
	Naming  *model.Naming `json:"naming" validate:"required"`
	model.Filter
	Branches []*model.BranchManifest         `json:"branches" validate:"dive"`
	Configs  []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func NewManifest(projectId int, apiHost string, fs filesystem.Fs) (*Manifest, error) {
	m := newManifest(projectId, apiHost, fs)
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func newManifest(projectId int, apiHost string, fs filesystem.Fs) *Manifest {
	return &Manifest{
		fs: fs,
		Content: &Content{
			Version:  1,
			Project:  model.Project{Id: projectId, ApiHost: apiHost},
			SortBy:   model.SortById,
			Naming:   model.DefaultNamingWithIds(),
			Filter:   model.DefaultFilter(),
			Branches: make([]*model.BranchManifest, 0),
			Configs:  make([]*model.ConfigManifestWithRows, 0),
		},
		records: *utils.NewOrderedMap(),
		lock:    &sync.Mutex{},
	}
}

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	// Exists?
	path := filesystem.Join(filesystem.MetadataDir, FileName)
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	m := newManifest(0, "", fs)
	if err := fs.ReadJsonFileTo(path, "manifest", &m.Content); err != nil {
		return nil, err
	}

	// Read all manifest records
	for _, branch := range m.Content.Branches {
		if err := m.trackRecord(branch); err != nil {
			return nil, err
		}
	}
	for _, config := range m.Content.Configs {
		if err := m.trackRecord(config.ConfigManifest); err != nil {
			return nil, err
		}
		for _, row := range config.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			if err := m.trackRecord(row); err != nil {
				return nil, err
			}
		}
	}

	// Validate
	if err := m.validate(); err != nil {
		return nil, err
	}

	// Connect records together and resolve parent path
	for _, key := range m.records.Keys() {
		r, _ := m.records.Get(key)
		record := r.(model.Record)
		if err := m.PersistRecord(record); err != nil {
			return nil, err
		}
	}

	// Track if manifest was changed after load
	m.loaded = true
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Save() error {
	// Convert records map to slices
	branchesMap := make(map[string]*model.BranchManifest)
	configsMap := make(map[string]*model.ConfigManifestWithRows)
	m.Content.Branches = make([]*model.BranchManifest, 0)
	m.Content.Configs = make([]*model.ConfigManifestWithRows, 0)

	// Ensure order of processing: branch, config, configRow
	m.sortRecords()

	for _, key := range m.records.Keys() {
		r, _ := m.records.Get(key)
		record := r.(model.Record)

		// Skip invalid (eg. missing config file)
		if record.State().IsInvalid() {
			continue
		}

		// Skip not persisted
		if !record.State().IsPersisted() {
			continue
		}

		// Generate content, we have to check if parent exists (eg. branch could have been deleted)
		switch v := record.(type) {
		case *model.BranchManifest:
			m.Content.Branches = append(m.Content.Branches, v)
			branchesMap[v.String()] = v
		case *model.ConfigManifest:
			_, found := branchesMap[v.BranchKey().String()]
			if found {
				config := &model.ConfigManifestWithRows{
					ConfigManifest: v,
					Rows:           make([]*model.ConfigRowManifest, 0),
				}
				configsMap[config.String()] = config
				m.Content.Configs = append(m.Content.Configs, config)
			}
		case *model.ConfigRowManifest:
			config, found := configsMap[v.ConfigKey().String()]
			if found {
				config.Rows = append(config.Rows, v)
			}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, record))
		}
	}

	// Validate
	err := m.validate()
	if err != nil {
		return err
	}

	// Write JSON file
	path := filesystem.Join(filesystem.MetadataDir, FileName)
	content, err := json.EncodeString(m.Content, true)
	if err != nil {
		return utils.PrefixError(`cannot encode manifest`, err)
	}
	file := filesystem.CreateFile(path, content)
	if err := m.fs.WriteFile(file); err != nil {
		return err
	}

	m.changed = false
	return nil
}

func (m *Manifest) IsObjectIgnored(object model.Object) bool {
	switch o := object.(type) {
	case *model.Branch:
		return !m.Content.AllowedBranches.IsBranchAllowed(o)
	case *model.Config:
		return m.Content.IgnoredComponents.Contains(o.ComponentId)
	case *model.ConfigRow:
		return m.Content.IgnoredComponents.Contains(o.ComponentId)
	}

	return false
}

func (m *Manifest) Fs() filesystem.Fs {
	return m.fs
}

func (m *Manifest) IsChanged() bool {
	return m.changed
}

func (m *Manifest) Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

func (m *Manifest) GetRecords() orderedmap.OrderedMap {
	m.sortRecords()
	return m.records
}

func (m *Manifest) MustGetRecord(key model.Key) model.Record {
	record, found := m.GetRecord(key)
	if !found {
		panic(fmt.Errorf("manifest record with key \"%s\"", key.String()))
	}
	return record
}

func (m *Manifest) GetRecord(key model.Key) (model.Record, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if r, found := m.records.Get(key.String()); found {
		return r.(model.Record), found
	}
	return nil, false
}

func (m *Manifest) CreateOrGetRecord(key model.Key) (record model.Record, found bool, err error) {
	// Get
	record, found = m.GetRecord(key)
	if found {
		return record, found, nil
	}

	// Create
	switch v := key.(type) {
	case model.BranchKey:
		record = &model.BranchManifest{BranchKey: v}
	case model.ConfigKey:
		record = &model.ConfigManifest{ConfigKey: v}
	case model.ConfigRowKey:
		record = &model.ConfigRowManifest{ConfigRowKey: v}
	default:
		panic(fmt.Errorf("unexpected type \"%s\"", key))
	}

	if err := m.trackRecord(record); err != nil {
		return nil, false, err
	}

	return record, false, nil
}

// PersistRecord - store a new or existing record, and marks it as persisted.
func (m *Manifest) PersistRecord(record model.Record) error {
	// Resolve parent path
	if !record.IsParentPathSet() {
		if err := m.ResolveParentPath(record); err != nil {
			return err
		}
	}

	// Attach record to the naming
	m.Naming.Attach(record.Key(), record.GetPathInProject())

	m.lock.Lock()
	defer m.lock.Unlock()

	// Mark persisted -> will be saved to manifest.json
	record.State().SetPersisted()

	m.records.Set(record.Key().String(), record)
	m.changed = true
	return nil
}

// trackRecord - store a new record and make sure it has unique key.
func (m *Manifest) trackRecord(record model.Record) error {
	// Resolve parent path and attach record to the naming
	if m.loaded {
		// All records must be loaded to resolve parent path
		if err := m.ResolveParentPath(record); err != nil {
			return err
		}
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	// Make sure the key is unique
	if _, exists := m.records.Get(record.Key().String()); exists {
		return fmt.Errorf(`duplicate %s in manifest`, record.Desc())
	}

	m.records.Set(record.Key().String(), record)
	return nil
}

func (m *Manifest) DeleteRecord(object model.WithKey) {
	m.DeleteRecordByKey(object.Key())
}

func (m *Manifest) DeleteRecordByKey(key model.Key) {
	record, found := m.GetRecord(key)
	if found {
		m.lock.Lock()
		defer m.lock.Unlock()
		record.State().SetDeleted()
		m.changed = m.changed || record.State().IsPersisted()
		m.records.Delete(key.String())
	}
}

func (m *Manifest) GetParent(record model.Record) (model.Record, error) {
	parentKey, err := record.ParentKey()
	if err != nil {
		return nil, err
	} else if parentKey == nil {
		return nil, nil
	}

	parent, found := m.GetRecord(parentKey)
	if !found {
		return nil, fmt.Errorf(`manifest record for %s not found, referenced from %s`, parentKey.Desc(), record.Desc())
	}
	return parent, nil
}

func (m *Manifest) ResolveParentPath(record model.Record) error {
	return m.doResolveParentPath(record, nil)
}

// doResolveParentPath recursive + fail on cyclic relations.
func (m *Manifest) doResolveParentPath(record, origin model.Record) error {
	if origin != nil && record.Key().String() == origin.Key().String() {
		return fmt.Errorf(`a cyclic relation was found when resolving path to %s`, origin.Desc())
	}

	if origin == nil {
		origin = record
	}

	parent, err := m.GetParent(record)
	switch {
	case err != nil:
		return err
	case parent != nil:
		// Recursively resolve the parent path, if it is not set
		if !parent.IsParentPathSet() {
			if err := m.doResolveParentPath(parent, origin); err != nil {
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

func (m *Manifest) validate() error {
	if err := validator.Validate(m); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}

// sortRecords in manifest + ensure order of processing: branch, config, configRow.
func (m *Manifest) sortRecords() {
	m.records.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		return a.Value().(model.Record).SortKey(m.SortBy) < b.Value().(model.Record).SortKey(m.SortBy)
	})
}
