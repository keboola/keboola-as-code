package manifest

import (
	"fmt"
	"path/filepath"
	"sync"

	"github.com/iancoleman/orderedmap"

	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
)

const (
	MetadataDir = ".keboola"
	FileName    = "manifest.json"
)

type Manifest struct {
	*Content    `validate:"required,dive"` // content of the file, updated only on load/save
	ProjectDir  string                     `validate:"required"` // project root
	MetadataDir string                     `validate:"required"` // inside ProjectDir
	changed     bool
	records     orderedmap.OrderedMap // common map for all: branches, configs and rows manifests
	lock        *sync.Mutex
}

type Content struct {
	Version int           `json:"version" validate:"required,min=1,max=1"`
	Project model.Project `json:"project" validate:"required"`
	SortBy  string        `json:"sortBy" validate:"oneof=id path"`
	Naming  model.Naming  `json:"naming" validate:"required"`
	model.Filter
	Branches []*model.BranchManifest         `json:"branches" validate:"dive"`
	Configs  []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func NewManifest(projectId int, apiHost string, projectDir, metadataDir string) (*Manifest, error) {
	m := newManifest(projectId, apiHost, projectDir, metadataDir)
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func newManifest(projectId int, apiHost string, projectDir, metadataDir string) *Manifest {
	return &Manifest{
		ProjectDir:  projectDir,
		MetadataDir: metadataDir,
		Content: &Content{
			Version:  1,
			Project:  model.Project{Id: projectId, ApiHost: apiHost},
			SortBy:   model.SortById,
			Naming:   model.DefaultNaming(),
			Filter:   model.DefaultFilter(),
			Branches: make([]*model.BranchManifest, 0),
			Configs:  make([]*model.ConfigManifestWithRows, 0),
		},
		records: *utils.NewOrderedMap(),
		lock:    &sync.Mutex{},
	}
}

func LoadManifest(projectDir string, metadataDir string) (*Manifest, error) {
	// Exists?
	path := filepath.Join(metadataDir, FileName)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", utils.RelPath(projectDir, path))
	}

	// Read JSON file
	m := newManifest(0, "", projectDir, metadataDir)
	if err := json.ReadFile(metadataDir, FileName, &m.Content, "manifest"); err != nil {
		return nil, err
	}

	// Connect together all manifest records
	for _, branch := range m.Content.Branches {
		if err := m.PersistRecord(branch); err != nil {
			return nil, err
		}
	}
	for _, config := range m.Content.Configs {
		if err := m.PersistRecord(config.ConfigManifest); err != nil {
			return nil, err
		}
		for _, row := range config.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			if err := m.PersistRecord(row); err != nil {
				return nil, err
			}
		}
	}

	// Track if was manifest changed after load
	m.changed = false

	// Validate
	if err := m.validate(); err != nil {
		return nil, err
	}

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
	if err := json.WriteFile(m.MetadataDir, FileName, m.Content, "manifest"); err != nil {
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

func (m *Manifest) IsChanged() bool {
	return m.changed
}

func (m *Manifest) RelativePath() string {
	return filepath.Join(m.MetadataDir, FileName)
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

	if err := m.TrackRecord(record); err != nil {
		return nil, false, err
	}

	return record, false, nil
}

func (m *Manifest) PersistRecord(record model.Record) error {
	if err := m.TrackRecord(record); err != nil {
		return err
	}

	m.Naming.Attach(record.Key(), record.RelativePath())
	record.State().SetPersisted()

	m.lock.Lock()
	defer m.lock.Unlock()
	m.changed = true
	return nil
}

func (m *Manifest) TrackRecord(record model.Record) error {
	// Resolve parent path, if record has been loaded from manifest.json
	if record.GetParentPath() == "" {
		if err := m.ResolveParentPath(record); err != nil {
			return err
		}
	}

	m.lock.Lock()
	defer m.lock.Unlock()
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
	parentKey := record.ParentKey()
	if parentKey == nil {
		return nil, nil
	}

	parent, found := m.GetRecord(parentKey)
	if !found {
		return nil, fmt.Errorf(`manifest record for %s not found, referenced from %s`, parentKey.Desc(), record.Desc())
	}
	return parent, nil
}

func (m *Manifest) ResolveParentPath(record model.Record) error {
	parent, err := m.GetParent(record)
	switch {
	case err != nil:
		return err
	case parent != nil:
		record.SetParentPath(parent.RelativePath())
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
