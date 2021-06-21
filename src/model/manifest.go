package model

import (
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/iancoleman/strcase"
	"github.com/spf13/cast"
	"go.uber.org/zap"
	"keboola-as-code/src/json"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
)

const (
	MetadataDir      = ".keboola"
	ManifestFileName = "manifest.json"
	MetaFile         = "meta.json"
	ConfigFile       = "config.json"
	RowsDir          = "rows"
)

type Manifest struct {
	ProjectDir  string                 `validate:"required"` // project root
	MetadataDir string                 `validate:"required"` // inside ProjectDir
	Registry    *orderedmap.OrderedMap // common map for all manifests: branches, configs and rows, guarantees uniqueness
	Content     *ManifestContent       // content of the file, updated only on LoadManifest() and Save()
	lock        *sync.Mutex
}

type ManifestContent struct {
	Version  int                       `json:"version" validate:"required,min=1,max=1"`
	Project  *ProjectManifest          `json:"project" validate:"required"`
	Branches []*BranchManifest         `json:"branches"`
	Configs  []*ConfigManifestWithRows `json:"configurations"`
}

type ObjectManifest interface {
	Kind() string
	KindAbbr() string
	UniqueKey() string
	Paths() ManifestPaths
}

type ManifestPaths struct {
	Path       string `json:"path" validate:"required"`
	ParentPath string `json:"-" validate:"required"` // generated, not in JSON
}

type ProjectManifest struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type BranchManifest struct {
	Id int `json:"id" validate:"required,min=1"`
	ManifestPaths
}

type ConfigManifest struct {
	BranchId    int    `json:"branchId" validate:"required"`
	ComponentId string `json:"componentId" validate:"required"`
	Id          string `json:"id" validate:"required,min=1"`
	ManifestPaths
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
	ManifestPaths
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
		Registry:    utils.EmptyOrderedMap(),
		Content: &ManifestContent{
			Version:  1,
			Project:  &ProjectManifest{Id: projectId, ApiHost: apiHost},
			Branches: make([]*BranchManifest, 0),
			Configs:  make([]*ConfigManifestWithRows, 0),
		},
		lock: &sync.Mutex{},
	}
}

func LoadManifest(projectDir string, metadataDir string) (*Manifest, error) {
	// Exists?
	path := filepath.Join(metadataDir, ManifestFileName)
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

	// Resolve paths, set parent IDs, store struct in Registry
	branchById := make(map[int]*BranchManifest)
	for _, branch := range m.Content.Branches {
		branch.ResolvePaths()
		branchById[branch.Id] = branch
		m.Registry.Set(branch.UniqueKey(), branch)
	}
	for _, configWithRows := range m.Content.Configs {
		config := configWithRows.ConfigManifest
		branch, found := branchById[config.BranchId]
		if !found {
			return nil, fmt.Errorf("branch \"%d\" not found in manifest - referenced from the config \"%s:%s\" in \"%s\"", config.BranchId, config.ComponentId, config.Id, path)
		}
		config.ResolvePaths(branch)
		m.Registry.Set(config.UniqueKey(), config)
		for _, row := range configWithRows.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			row.ResolvePaths(config)
			m.Registry.Set(row.UniqueKey(), row)
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

func (m *Manifest) Path() string {
	return filepath.Join(m.MetadataDir, ManifestFileName)
}

func (m *Manifest) Save() error {
	// Sort
	m.Registry.SortKeys(sort.Strings)

	// Convert registry to slices
	configsMap := make(map[string]*ConfigManifestWithRows)
	m.Content.Branches = make([]*BranchManifest, 0)
	m.Content.Configs = make([]*ConfigManifestWithRows, 0)
	for _, key := range m.Registry.Keys() {
		item, _ := m.Registry.Get(key)
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
	return os.WriteFile(m.Path(), data, 0650)
}

func (m *Manifest) LoadModel(om ObjectManifest, model interface{}) error {
	paths := om.Paths()
	modelType := reflect.TypeOf(model).Elem()
	modelValue := reflect.ValueOf(model).Elem()

	// Load meta file
	metaFields := utils.GetFieldsWithTag("metaFile", "true", modelType, model)
	if len(metaFields) > 0 {
		metadataContent := make(map[string]interface{})
		metadataFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), MetaFile)
		if err := readJsonFile(m.ProjectDir, metadataFile, &metadataContent, om.Kind()+" metadata"); err != nil {
			return err
		}

		for _, field := range metaFields {
			// Use JSON name if present
			name := field.Name
			jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
			if jsonName != "" {
				name = jsonName
			}

			// Set value, some value are optional, model will be validated later
			if value, ok := metadataContent[name]; ok {
				modelValue.FieldByName(field.Name).Set(reflect.ValueOf(value))
			}
		}
	}

	// Load config file
	configFields := utils.GetFieldsWithTag("configFile", "true", modelType, model)
	if len(configFields) > 1 {
		panic(fmt.Errorf("struct \"%s\" has multiple fields with tag `configFile:\"true\"`, but only one allowed", modelType))
	} else if len(configFields) == 1 {
		configFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), ConfigFile)
		content := utils.EmptyOrderedMap()
		if err := readJsonFile(m.ProjectDir, configFile, &content, om.Kind()); err != nil {
			return err
		}
		modelValue.FieldByName(configFields[0].Name).Set(reflect.ValueOf(content))
	}

	return nil
}

func (m *Manifest) SaveModel(om ObjectManifest, model interface{}, logger *zap.SugaredLogger) error {
	paths := om.Paths()

	// Add to manifest content
	m.lock.Lock()
	m.Registry.Set(om.UniqueKey(), om)
	m.lock.Unlock()

	// Mkdir
	if err := os.MkdirAll(paths.AbsolutePath(m.ProjectDir), 06500); err != nil {
		return err
	}

	// Write metadata file
	if metadata := m.toMetadataFile(model); metadata != nil {
		metadataFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), MetaFile)
		metadataJson, err := json.Encode(metadata, true)
		if err != nil {
			return err
		}
		if err := os.WriteFile(metadataFile, metadataJson, 0650); err != nil {
			return err
		}
		logger.Debugf("Saved \"%s\"", utils.RelPath(m.ProjectDir, metadataFile))
	}

	// Write config file
	if configContent := m.toConfigFile(model); configContent != nil {
		configFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), ConfigFile)
		configJson, err := json.Encode(configContent, true)
		if err != nil {
			return err
		}
		if err := os.WriteFile(configFile, configJson, 0650); err != nil {
			return err
		}
		logger.Debugf("Saved \"%s\"", utils.RelPath(m.ProjectDir, configFile))
	}

	return nil
}

func (m *Manifest) DeleteModel(om ObjectManifest, model interface{}, logger *zap.SugaredLogger) error {
	paths := om.Paths()

	// Remove from manifest content
	m.lock.Lock()
	m.Registry.Delete(om.UniqueKey())
	m.lock.Unlock()

	// Delete metadata file
	if metadata := m.toMetadataFile(model); metadata != nil {
		metadataFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), MetaFile)
		if err := os.Remove(metadataFile); err != nil {
			return err
		}
		logger.Debugf("Removed \"%s\"", metadataFile)
	}

	// Delete config file
	if configContent := m.toConfigFile(model); configContent != nil {
		configFile := filepath.Join(paths.AbsolutePath(m.ProjectDir), ConfigFile)
		if err := os.Remove(configFile); err != nil {
			return err
		}
		logger.Debugf("Removed \"%s\"", configFile)
	}

	return nil
}

func (m *Manifest) validate() error {
	if err := validator.Validate(m); err != nil {
		errStr := strings.ReplaceAll(err.Error(), "Content.", "")
		return fmt.Errorf("manifest is not valid: %s", errStr)
	}
	return nil
}

func (m *Manifest) toMetadataFile(model interface{}) *orderedmap.OrderedMap {
	target := orderedmap.New()
	modelType := reflect.TypeOf(model).Elem()
	modelValue := reflect.ValueOf(model).Elem()
	for _, field := range utils.GetFieldsWithTag("metaFile", "true", modelType, model) {
		// Use JSON name if present
		name := field.Name
		jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
		if jsonName != "" {
			name = jsonName
		}

		// Get field value
		target.Set(name, modelValue.FieldByName(field.Name).Interface())
	}
	return target
}

func (m *Manifest) toConfigFile(model interface{}) *orderedmap.OrderedMap {
	modelType := reflect.TypeOf(model).Elem()
	modelValue := reflect.ValueOf(model).Elem()
	fields := utils.GetFieldsWithTag("configFile", "true", modelType, model)

	// Check number of fields
	if len(fields) > 1 {
		panic(fmt.Errorf("struct \"%s\" has multiple fields with tag `configFile:\"true\"`, but only one allowed", modelType))
	} else if len(fields) == 0 {
		return nil
	}

	// Ok, return map
	return modelValue.FieldByName(fields[0].Name).Interface().(*orderedmap.OrderedMap)
}

func (o ManifestPaths) Paths() ManifestPaths {
	return o
}

func (o *ManifestPaths) RelativePath() string {
	return filepath.Join(o.ParentPath, o.Path)
}

func (o *ManifestPaths) AbsolutePath(projectDir string) string {
	return filepath.Join(projectDir, o.RelativePath())
}

func (o *ManifestPaths) MetadataFilePath() string {
	return filepath.Join(o.RelativePath(), MetaFile)
}

func (o *ManifestPaths) ConfigFilePath() string {
	return filepath.Join(o.RelativePath(), ConfigFile)
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

func (b *BranchManifest) UniqueKey() string {
	return fmt.Sprintf("01_branch_%d", b.Id)
}

func (c *ConfigManifest) UniqueKey() string {
	return fmt.Sprintf("02_config_%d_%s_%s", c.BranchId, c.ComponentId, c.Id)
}

func (r *ConfigRowManifest) UniqueKey() string {
	return fmt.Sprintf("03_row_%d_%s_%s_%s", r.BranchId, r.ComponentId, r.ConfigId, r.Id)
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

func (b *BranchManifest) ToModel(m *Manifest) (*Branch, error) {
	branch := &Branch{Id: b.Id}
	if err := m.LoadModel(b, branch); err != nil {
		return nil, err
	}
	return branch, nil
}

func (c *ConfigManifest) ToModel(m *Manifest) (*Config, error) {
	config := &Config{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.Id}
	if err := m.LoadModel(c, config); err != nil {
		return nil, err
	}
	return config, nil
}

func (r *ConfigRowManifest) ToModel(m *Manifest) (*ConfigRow, error) {
	row := &ConfigRow{BranchId: r.BranchId, ComponentId: r.ComponentId, ConfigId: r.ConfigId, Id: r.Id}
	if err := m.LoadModel(r, row); err != nil {
		return nil, err
	}
	return row, nil
}

func (b *Branch) GenerateManifest() *BranchManifest {
	manifest := &BranchManifest{Id: b.Id}
	if b.IsDefault {
		manifest.Path = "main"
	} else {
		manifest.Path = generatePath(cast.ToString(b.Id), b.Name)
	}
	manifest.ResolvePaths()
	return manifest
}

func (c *Config) GenerateManifest(b *BranchManifest, component *Component) *ConfigManifest {
	manifest := &ConfigManifest{BranchId: c.BranchId, ComponentId: c.ComponentId, Id: c.Id}
	manifest.Path = filepath.Join(component.Type, c.ComponentId, generatePath(c.Id, c.Name))
	manifest.ResolvePaths(b)
	return manifest
}

func (r *ConfigRow) GenerateManifest(c *ConfigManifest) *ConfigRowManifest {
	manifest := &ConfigRowManifest{BranchId: r.BranchId, ComponentId: r.ComponentId, ConfigId: r.ConfigId, Id: r.Id}
	manifest.Path = generatePath(r.Id, r.Name)
	manifest.ResolvePaths(c)
	return manifest
}

func readJsonFile(projectDir string, path string, v interface{}, errPrefix string) error {
	// Read meta file
	if !utils.IsFile(path) {
		return fmt.Errorf("%s JSON file \"%s\" not found", errPrefix, utils.RelPath(projectDir, path))
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read %s JSON file \"%s\"", errPrefix, utils.RelPath(projectDir, path))
	}

	// Decode meta file
	err = json.Decode(content, v)
	if err != nil {
		return fmt.Errorf("%s JSON file \"%s\" is invalid: %s", errPrefix, utils.RelPath(projectDir, path), err)
	}
	return nil
}

func generatePath(id string, name string) string {
	name = regexp.
		MustCompile(`[^a-zA-Z0-9-]]`).
		ReplaceAllString(strcase.ToDelimited(name, '-'), "-")
	return id + "-" + name
}
