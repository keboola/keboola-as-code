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
	"sort"
	"strings"
	"sync"
)

const (
	MetadataDir = ".keboola"
	FileName    = "manifest.json"
	MetaFile    = "meta.json"
	ConfigFile  = "config.json"
	RowsDir     = "rows"
	SortById    = "id"
	SortByPath  = "path"
)

type Manifest struct {
	*Content                           // content of the file, updated only on LoadManifest() and Save()
	ProjectDir  string                 `validate:"required"` // project root
	MetadataDir string                 `validate:"required"` // inside ProjectDir
	records     *orderedmap.OrderedMap // common map for all: branches, configs and rows manifests
	lock        *sync.Mutex
}

type Content struct {
	Version  int                       `json:"version" validate:"required,min=1,max=1"`
	Project  *Project                  `json:"project" validate:"required"`
	SortBy   string                    `json:"sortBy" validate:"oneof=id path"`
	Naming   *LocalNaming              `json:"naming" validate:"required"`
	Branches []*BranchManifest         `json:"branches"`
	Configs  []*ConfigManifestWithRows `json:"configurations"`
}

type Record interface {
	Kind() string
	KindAbbr() string
	UniqueKey(sort string) string
	GetPaths() *Paths
	MetaFilePath() string
	ConfigFilePath() string
}

type Paths struct {
	Path       string `json:"path" validate:"required"`
	ParentPath string `json:"-" validate:"required"` // generated, not in JSON
}

type Project struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type BranchManifest struct {
	Id int `json:"id" validate:"required,min=1"`
	Paths
}

type ConfigManifest struct {
	BranchId    int    `json:"branchId" validate:"required"`
	ComponentId string `json:"componentId" validate:"required"`
	Id          string `json:"id" validate:"required"`
	Paths
}

type ConfigManifestWithRows struct {
	*ConfigManifest
	Rows []*ConfigRowManifest `json:"rows"`
}

type ConfigRowManifest struct {
	Id          string `json:"id" validate:"required,min=1"`
	BranchId    int    `json:"-" validate:"required"` // generated, not in JSON
	ComponentId string `json:"-" validate:"required"` // generated, not in JSON
	ConfigId    string `json:"-" validate:"required"` // generated, not in JSON
	Paths
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
		records:     utils.EmptyOrderedMap(),
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

	// Load file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read from manifest \"%s\": %s", utils.RelPath(projectDir, path), err)
	}

	// Decode JSON
	m := newManifest(0, "", projectDir, metadataDir)
	err = json.Decode(data, m.Content)
	if err != nil {
		return nil, fmt.Errorf("manifest \"%s\" is not valid: %s", utils.RelPath(projectDir, path), err)
	}

	// Resolve paths, set parent IDs, store struct in records
	branchById := make(map[int]*BranchManifest)
	for _, branch := range m.Content.Branches {
		branch.ResolvePaths()
		branchById[branch.Id] = branch
		m.records.Set(branch.UniqueKey(m.SortBy), branch)
	}
	for _, configWithRows := range m.Content.Configs {
		config := configWithRows.ConfigManifest
		branch, found := branchById[config.BranchId]
		if !found {
			return nil, fmt.Errorf("branch \"%d\" not found in manifest - referenced from the config \"%s:%s\" in \"%s\"", config.BranchId, config.ComponentId, config.Id, path)
		}
		config.ResolvePaths(branch)
		m.records.Set(config.UniqueKey(m.SortBy), config)
		for _, row := range configWithRows.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			row.ResolvePaths(config)
			m.records.Set(row.UniqueKey(m.SortBy), row)
		}
	}

	// Validate
	err = m.validate()
	if err != nil {
		return nil, err
	}

	// Return
	return m, nil
}

func (m *Manifest) Save() error {
	// Sort
	m.records.SortKeys(sort.Strings)

	// Convert registry to slices
	configsMap := make(map[string]*ConfigManifestWithRows)
	m.Content.Branches = make([]*BranchManifest, 0)
	m.Content.Configs = make([]*ConfigManifestWithRows, 0)
	for _, key := range m.records.Keys() {
		item, _ := m.records.Get(key)
		switch v := item.(type) {
		case *BranchManifest:
			m.Content.Branches = append(m.Content.Branches, v)
		case *ConfigManifest:
			mapKey := fmt.Sprintf("%d_%s_%s", v.BranchId, v.ComponentId, v.Id)
			config := &ConfigManifestWithRows{v, make([]*ConfigRowManifest, 0)}
			configsMap[mapKey] = config
			m.Content.Configs = append(m.Content.Configs, config)
		case *ConfigRowManifest:
			mapKey := fmt.Sprintf("%d_%s_%s", v.BranchId, v.ComponentId, v.ConfigId)
			config, found := configsMap[mapKey]
			if !found {
				panic(fmt.Errorf(`config with key "%s" not found"`, mapKey))
			}
			config.Rows = append(config.Rows, v)
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, item))
		}
	}

	// Validate
	err := m.validate()
	if err != nil {
		return err
	}

	// Encode JSON
	data, err := json.Encode(m.Content, true)
	if err != nil {
		return err
	}

	// Write file
	return os.WriteFile(m.Path(), data, 0644)
}

func (m *Manifest) Path() string {
	return filepath.Join(m.MetadataDir, FileName)
}

func (m *Manifest) validate() error {
	if err := validator.Validate(m); err != nil {
		errStr := strings.ReplaceAll(err.Error(), "Content.", "")
		return fmt.Errorf("manifest is not valid: %s", errStr)
	}
	return nil
}

func (m *Manifest) GetRecords() orderedmap.OrderedMap {
	return *m.records
}

func (m *Manifest) GetRecord(key string) (interface{}, bool) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.records.Get(key)
}

func (m *Manifest) AddRecord(record Record) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.records.Set(record.UniqueKey(m.SortBy), record)
}

func (m *Manifest) DeleteRecord(record Record) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.records.Delete(record.UniqueKey(m.SortBy))
}

func (m *Manifest) DeleteRecordByKey(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.records.Delete(key)
}

func (o *Paths) GetPaths() *Paths {
	return o
}

func (o *Paths) RelativePath() string {
	return filepath.Join(o.ParentPath, o.Path)
}

func (o *Paths) AbsolutePath(projectDir string) string {
	return filepath.Join(projectDir, o.RelativePath())
}

func (o *Paths) MetaFilePath() string {
	return filepath.Join(o.RelativePath(), MetaFile)
}

func (o *Paths) ConfigFilePath() string {
	return filepath.Join(o.RelativePath(), ConfigFile)
}

func NewBranchManifest(naming *LocalNaming, branch *model.Branch) *BranchManifest {
	manifest := &BranchManifest{Id: branch.Id}
	if branch.IsDefault {
		manifest.Path = "main"
	} else {
		manifest.Path = naming.BranchPath(branch)
	}
	manifest.ResolvePaths()
	return manifest
}

func NewConfigManifest(naming *LocalNaming, branchManifest *BranchManifest, component *model.Component, c *model.Config) *ConfigManifest {
	manifest := &ConfigManifest{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.Id}
	manifest.Path = naming.ConfigPath(component, c)
	manifest.ResolvePaths(branchManifest)
	return manifest
}

func NewConfigRowManifest(naming *LocalNaming, configManifest *ConfigManifest, r *model.ConfigRow) *ConfigRowManifest {
	manifest := &ConfigRowManifest{BranchId: r.BranchId, ComponentId: r.ComponentId, ConfigId: r.ConfigId, Id: r.Id}
	manifest.Path = naming.ConfigRowPath(r)
	manifest.ResolvePaths(configManifest)
	return manifest
}

func (b *BranchManifest) Kind() string {
	return "branch"
}

func (c *ConfigManifest) Kind() string {
	return "config"
}

func (r *ConfigRowManifest) Kind() string {
	return "config row"
}

func (b *BranchManifest) KindAbbr() string {
	return "B"
}

func (c *ConfigManifest) KindAbbr() string {
	return "C"
}

func (r *ConfigRowManifest) KindAbbr() string {
	return "R"
}

func (b *BranchManifest) UniqueKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("01_branch_%s", b.RelativePath())
	} else {
		return fmt.Sprintf("01_branch_%d", b.Id)
	}

}

func (c *ConfigManifest) UniqueKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("02_config_%s", c.RelativePath())
	} else {
		return fmt.Sprintf("02_config_%d_%s_%s", c.BranchId, c.ComponentId, c.Id)
	}

}

func (r *ConfigRowManifest) UniqueKey(sort string) string {
	if sort == SortByPath {
		return fmt.Sprintf("03_row_%s", r.RelativePath())
	} else {
		return fmt.Sprintf("03_row_%d_%s_%s_%s", r.BranchId, r.ComponentId, r.ConfigId, r.Id)
	}

}

func (b *BranchManifest) ResolvePaths() {
	b.ParentPath = ""
}

func (c *ConfigManifest) ResolvePaths(b *BranchManifest) {
	c.ParentPath = filepath.Join(b.ParentPath, b.Path)
}

func (r *ConfigRowManifest) ResolvePaths(c *ConfigManifest) {
	r.ParentPath = filepath.Join(c.ParentPath, c.Path, RowsDir)
}
