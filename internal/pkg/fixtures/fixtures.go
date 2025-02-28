package fixtures

import (
	"encoding/json"
	"os"
	"runtime"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type ProjectSnapshot struct {
	Branches  []*BranchWithConfigs `json:"branches"`
	Schedules []*Schedule          `json:"schedules,omitempty"`
	Sandboxes []*Sandbox           `json:"sandboxes,omitempty"`
	Buckets   []*Bucket            `json:"buckets,omitempty"`
	Files     []*File              `json:"files,omitempty"`
}

type Branch struct {
	Name        string            `json:"name" validate:"required"`
	Description string            `json:"description"`
	IsDefault   bool              `json:"isDefault"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

type BranchState struct {
	*Branch `json:"branch" validate:"required"`
	Configs []string `json:"configs"`
}

type BranchWithConfigs struct {
	*Branch `json:"branch" validate:"required"`
	Configs []*Config `json:"configs"`
}

type Schedule struct {
	Name string `json:"name"`
}

type Sandbox struct {
	Name string `json:"name" validate:"required"`
	Type string `json:"type" validate:"required"`
	Size string `json:"size,omitempty"`
}

type Bucket struct {
	ID          keboola.BucketID `json:"id"`
	URI         string           `json:"uri"`
	DisplayName string           `json:"displayName"`
	Description string           `json:"description"`
	Tables      []*Table         `json:"tables"`
}

type Table struct {
	ID          keboola.TableID          `json:"id"`
	URI         string                   `json:"uri"`
	Name        string                   `json:"name"`
	DisplayName string                   `json:"displayName"`
	PrimaryKey  []string                 `json:"primaryKey"`
	Definition  *keboola.TableDefinition `json:"definition,omitempty"`
	Columns     []string                 `json:"columns"`
	Rows        [][]string               `json:"rows,omitempty"`
	RowsCount   uint64                   `json:"rowsCount,omitempty"`
}

type File struct {
	Name        string      `json:"name"`
	Content     string      `json:"content,omitempty"`
	Tags        []string    `json:"tags"`
	IsSliced    bool        `json:"isSliced"`
	IsEncrypted bool        `json:"isEncrypted"`
	IsPermanent bool        `json:"isPermanent"`
	Slices      []FileSlice `json:"slices,omitempty"`
}

type FileSlice struct {
	Name    string `json:"name"`
	Content string `json:"content"`
}

type Config struct {
	ComponentID       keboola.ComponentID    `json:"componentId" validate:"required"`
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription,omitempty"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
	Rows              []*ConfigRow           `json:"rows"`
	Metadata          map[string]string      `json:"metadata,omitempty"`
	IsDisabled        bool                   `json:"isDisabled"`
}

type ConfigRow struct {
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription,omitempty"`
	IsDisabled        bool                   `json:"isDisabled"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
}

type BackendDefinition struct {
	Type string `json:"type"`
}

type StateFile struct {
	Backend              *BackendDefinition `json:"backend,omitempty"`
	LegacyTransformation bool               `json:"legacyTransformation,omitempty"`
	AllBranchesConfigs   []string           `json:"allBranchesConfigs" validate:"required"`
	Branches             []*BranchState     `json:"branches" validate:"required"`
	Buckets              []*Bucket          `json:"buckets,omitempty"`
	Sandboxes            []*Sandbox         `json:"sandboxes,omitempty"`
	Files                []*File            `json:"files,omitempty"`
	Envs                 map[string]string  `json:"envs,omitempty"` // additional ENVs
}

// ToAPI maps fixture to model.Branch.
func (b *Branch) ToAPI() *keboola.Branch {
	branch := &keboola.Branch{}
	branch.Name = b.Name
	branch.Description = b.Description
	branch.IsDefault = b.IsDefault
	return branch
}

// ToAPI maps fixture to model.Config.
func (c *Config) ToAPI() *keboola.ConfigWithRows {
	config := &keboola.ConfigWithRows{Config: &keboola.Config{}}
	config.ComponentID = c.ComponentID
	config.Name = c.Name
	config.Description = "test fixture"
	config.ChangeDescription = "created by test"
	config.Content = c.Content
	config.IsDisabled = c.IsDisabled

	for _, r := range c.Rows {
		config.Rows = append(config.Rows, r.ToAPI())
	}

	return config
}

// ToAPI maps fixture to model.Config.
func (r *ConfigRow) ToAPI() *keboola.ConfigRow {
	row := &keboola.ConfigRow{}
	row.Name = r.Name
	row.Description = "test fixture"
	row.ChangeDescription = "created by test"
	row.IsDisabled = r.IsDisabled
	row.Content = r.Content
	return row
}

func (b *Branch) String() string {
	return b.Description
}

func (c *Config) String() string {
	return c.Description
}

func (r *ConfigRow) String() string {
	return r.Description
}

func (s *Sandbox) String() string {
	return s.Type + "_" + s.Size
}

func (b *Branch) ObjectName() string {
	return b.Name
}

func (c *Config) ObjectName() string {
	return c.Name
}

func (r *ConfigRow) ObjectName() string {
	return r.Name
}

func (s *Sandbox) ObjectName() string {
	return s.Name
}

func MinimalProjectFs(t *testing.T) filesystem.Fs {
	t.Helper()

	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)

	// Create Fs
	inputDir := filesystem.Join(testDir, "local", "minimal")
	fs := aferofs.NewMemoryFsFrom(inputDir)

	// Replace ENVs
	envs := env.Empty()
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	envs.Set("LOCAL_STATE_MAIN_BRANCH_ID", "123")
	envs.Set("LOCAL_STATE_GENERIC_CONFIG_ID", "456")
	err := testhelper.ReplaceEnvsDir(t.Context(), fs, `/`, envs)
	require.NoError(t, err)

	return fs
}

func LoadStateFile(fs filesystem.Fs, path string) (*StateFile, error) {
	data, err := fs.ReadFile(context.Background(), filesystem.NewFileDef(path))
	if err != nil {
		return nil, errors.Errorf(`cannot load test project state file "%s": %w`, path, err)
	}

	stateFile := &StateFile{}
	if err := json.Unmarshal([]byte(data.Content), stateFile); err != nil {
		return nil, errors.Errorf("cannot parse test project state file \"%s\": %w", path, err)
	}

	// Check if main branch defined
	// Create definition if not exists
	found := false
	for _, branch := range stateFile.Branches {
		if branch.Branch.IsDefault {
			found = true
			break
		}
	}
	if !found {
		stateFile.Branches = append(stateFile.Branches, &BranchState{
			Branch: &Branch{Name: "Main", IsDefault: true},
		})
	}

	return stateFile, nil
}

// LoadConfig loads config from the file.
func LoadConfig(name string) *Config {
	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)

	// Load file
	path := filesystem.Join(testDir, "configs", name+".json")
	data, err := os.ReadFile(path) // nolint: forbidigo
	if err != nil {
		panic(errors.Errorf(`cannot load test confg file "%s": %w`, path, err))
	}

	// Parse file
	fixture := &Config{}
	if err := json.Unmarshal(data, fixture); err != nil {
		panic(errors.Errorf("cannot parse test config file \"%s\": %w", path, err))
	}

	return fixture
}
