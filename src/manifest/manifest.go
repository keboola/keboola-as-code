package manifest

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	MetadataDir = ".keboola"
	FileName    = "manifest.json"
	MetaFile    = "meta.json"
	ConfigFile  = "config.json"
	SortById    = "id"
	SortByPath  = "path"
)

type Manifest struct {
	*Content    `validate:"required,dive"` // content of the file, updated only on LoadManifest() and Save()
	ProjectDir  string                     `validate:"required"` // project root
	MetadataDir string                     `validate:"required"` // inside ProjectDir
	changed     bool                       // is content changed?
	records     orderedmap.OrderedMap      // common map for all: branches, configs and rows manifests
	lock        *sync.Mutex
}

type Content struct {
	Version  int                       `json:"version" validate:"required,min=1,max=1"`
	Project  *Project                  `json:"project" validate:"required"`
	SortBy   string                    `json:"sortBy" validate:"oneof=id path"`
	Naming   *LocalNaming              `json:"naming" validate:"required"`
	Branches []*BranchManifest         `json:"branches" validate:"dive"`
	Configs  []*ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

type Record interface {
	Kind() model.Kind           // eg. branch, config, config row -> used in logs
	Key() model.Key             // unique key for map -> for fast access
	SortKey(sort string) string // unique key for sorting
	RelativePath() string       // path to the object directory
	MetaFilePath() string       // path to the meta.json file
	ConfigFilePath() string     // path to the config.json file
	State() *RecordState
}

type RecordState struct {
	Invalid   bool // if true, object files are not valid, eg. missing file, invalid JSON, ...
	NotFound  bool // if true, object directory is not present in the filesystem
	Persisted bool // if true, record will be part of the manifest when saved
}

type Paths struct {
	Path       string `json:"path" validate:"required"`
	ParentPath string `json:"-"`
}

type Project struct {
	Id      int    `json:"id" validate:"required"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type BranchManifest struct {
	RecordState `json:"-"`
	model.BranchKey
	Paths
}

type ConfigManifest struct {
	RecordState `json:"-"`
	model.ConfigKey
	Paths
}

type ConfigRowManifest struct {
	RecordState `json:"-"`
	model.ConfigRowKey
	Paths
}

type ConfigManifestWithRows struct {
	*ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

func NewManifest(projectId int, apiHost string, projectDir, metadataDir string) (*Manifest, error) {
	m := newManifest(projectId, apiHost, projectDir, metadataDir)
	err := m.validate()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func newManifest(projectId int, apiHost string, projectDir, metadataDir string) *Manifest {
	return &Manifest{
		ProjectDir:  projectDir,
		MetadataDir: metadataDir,
		records:     *utils.NewOrderedMap(),
		Content: &Content{
			Version:  1,
			Project:  &Project{Id: projectId, ApiHost: apiHost},
			SortBy:   SortById,
			Naming:   DefaultNaming(),
			Branches: make([]*BranchManifest, 0),
			Configs:  make([]*ConfigManifestWithRows, 0),
		},
		lock: &sync.Mutex{},
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

	// Resolve parent path, set parent IDs, store records
	branchById := make(map[int]*BranchManifest)
	for _, branch := range m.Content.Branches {
		branch.ResolveParentPath()
		branchById[branch.Id] = branch
		m.PersistRecord(branch)
	}
	for _, configWithRows := range m.Content.Configs {
		config := configWithRows.ConfigManifest
		branch, found := branchById[config.BranchId]
		if !found {
			return nil, fmt.Errorf("branch \"%d\" not found in the manifest - referenced from the config \"%s:%s\" in \"%s\"", config.BranchId, config.ComponentId, config.Id, path)
		}
		config.ResolveParentPath(branch)
		m.PersistRecord(config)
		for _, row := range configWithRows.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			row.ResolveParentPath(config)
			m.PersistRecord(row)
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
	branchesMap := make(map[string]*BranchManifest)
	configsMap := make(map[string]*ConfigManifestWithRows)
	m.Content.Branches = make([]*BranchManifest, 0)
	m.Content.Configs = make([]*ConfigManifestWithRows, 0)

	// Ensure order of processing: branch, config, configRow
	m.sortRecords()

	for _, key := range m.records.Keys() {
		r, _ := m.records.Get(key)
		record := r.(Record)

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
		case *BranchManifest:
			m.Content.Branches = append(m.Content.Branches, v)
			branchesMap[v.String()] = v
		case *ConfigManifest:
			_, found := branchesMap[v.BranchKey().String()]
			if found {
				config := &ConfigManifestWithRows{v, make([]*ConfigRowManifest, 0)}
				configsMap[config.String()] = config
				m.Content.Configs = append(m.Content.Configs, config)
			}
		case *ConfigRowManifest:
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

func (m *Manifest) IsChanged() bool {
	return m.changed
}

func (m *Manifest) Path() string {
	return filepath.Join(m.MetadataDir, FileName)
}

func (m *Manifest) validate() error {
	if err := validator.Validate(m); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}

// sortRecords in manifest + ensure order of processing: branch, config, configRow
func (m *Manifest) sortRecords() {
	m.records.Sort(func(a *orderedmap.Pair, b *orderedmap.Pair) bool {
		return a.Value().(Record).SortKey(m.SortBy) < b.Value().(Record).SortKey(m.SortBy)
	})
}

func (m *Manifest) GetRecords() orderedmap.OrderedMap {
	m.sortRecords()
	return m.records
}

func (m *Manifest) MustGetRecord(key model.Key) Record {
	record, found := m.GetRecord(key)
	if !found {
		panic(fmt.Errorf("manifest record with key \"%s\"", key.String()))
	}
	return record
}

func (m *Manifest) GetRecord(key model.Key) (Record, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	if r, found := m.records.Get(key.String()); found {
		return r.(Record), found
	}
	return nil, false
}

func (m *Manifest) CreateOrGetRecord(key model.Key) (record Record) {
	// Get
	record, found := m.GetRecord(key)
	if found {
		return record
	}

	// Create
	switch v := key.(type) {
	case model.BranchKey:
		record = &BranchManifest{BranchKey: v}
	case model.ConfigKey:
		record = &ConfigManifest{ConfigKey: v}
	case model.ConfigRowKey:
		record = &ConfigRowManifest{ConfigRowKey: v}
	default:
		panic(fmt.Errorf("unexpected type \"%s\"", key))
	}
	m.TrackRecord(record)
	return record
}

func (m *Manifest) PersistRecord(record Record) {
	record.State().SetPersisted()
	m.TrackRecord(record)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.changed = true
}

func (m *Manifest) TrackRecord(record Record) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.records.Set(record.Key().String(), record)
}

func (m *Manifest) DeleteRecord(value model.ValueWithKey) {
	m.DeleteRecordByKey(value.Key())
}

func (m *Manifest) DeleteRecordByKey(key model.Key) {
	record, found := m.GetRecord(key)
	if found {
		m.lock.Lock()
		defer m.lock.Unlock()
		m.changed = m.changed || record.State().IsPersisted()
		m.records.Delete(key.String())
	}
}

func (o Paths) GetPaths() Paths {
	return o
}

func (o Paths) RelativePath() string {
	return filepath.Join(
		strings.ReplaceAll(o.ParentPath, "/", string(os.PathSeparator)),
		strings.ReplaceAll(o.Path, "/", string(os.PathSeparator)),
	)
}

func (o Paths) AbsolutePath(projectDir string) string {
	return filepath.Join(projectDir, o.RelativePath())
}

func (o Paths) MetaFilePath() string {
	return filepath.Join(o.RelativePath(), MetaFile)
}

func (o Paths) ConfigFilePath() string {
	return filepath.Join(o.RelativePath(), ConfigFile)
}

func (b BranchManifest) Kind() model.Kind {
	return model.Kind{Name: "branch", Abbr: "B"}
}

func (c ConfigManifest) Kind() model.Kind {
	return model.Kind{Name: "config", Abbr: "C"}
}

func (r ConfigRowManifest) Kind() model.Kind {
	return model.Kind{Name: "config row", Abbr: "R"}
}

func (s *RecordState) State() *RecordState {
	return s
}

func (s *RecordState) IsNotFound() bool {
	return s.NotFound
}

func (s *RecordState) SetNotFound() {
	s.NotFound = true
}

func (s *RecordState) IsInvalid() bool {
	return s.Invalid
}

func (s *RecordState) SetInvalid() {
	s.Invalid = true
}

func (s *RecordState) IsPersisted() bool {
	return s.Persisted
}

func (s *RecordState) SetPersisted() {
	s.Invalid = false
	s.Persisted = true
}

func (b *BranchManifest) ResolveParentPath() {
	b.ParentPath = ""
}

func (c *ConfigManifest) ResolveParentPath(branchManifest *BranchManifest) {
	c.ParentPath = filepath.Join(branchManifest.ParentPath, branchManifest.Path)
}

func (r *ConfigRowManifest) ResolveParentPath(configManifest *ConfigManifest) {
	r.ParentPath = filepath.Join(configManifest.ParentPath, configManifest.Path)
}

func (b BranchManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("01_branch_%s", b.RelativePath())
	} else {
		return b.BranchKey.String()
	}

}

func (c ConfigManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("02_config_%s", c.RelativePath())
	} else {
		return c.ConfigKey.String()
	}

}

func (r ConfigRowManifest) SortKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("03_row_%s", r.RelativePath())
	} else {
		return r.ConfigRowKey.String()
	}

}
