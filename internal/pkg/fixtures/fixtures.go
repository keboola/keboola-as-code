package fixtures

import (
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/iancoleman/orderedmap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type ProjectSnapshot struct {
	Branches  []*BranchWithConfigs `json:"branches"`
	Schedules []*ScheduleConfig    `json:"schedules,omitempty"`
}

type Branch struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description"`
	IsDefault   bool   `json:"isDefault"`
}

type BranchState struct {
	*Branch `json:"branch" validate:"required"`
	Configs []string `json:"configs"`
}

type BranchWithConfigs struct {
	*Branch `json:"branch" validate:"required"`
	Configs []*Config `json:"configs"`
}

type ScheduleConfig struct {
	Name string `json:"name"`
}

type Config struct {
	ComponentId       string                 `json:"componentId" validate:"required"`
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
	Rows              []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name              string                 `json:"name"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
}

type StateFile struct {
	AllBranchesConfigs []string          `json:"allBranchesConfigs" validate:"required"`
	Branches           []*BranchState    `json:"branches" validate:"required"`
	Envs               map[string]string `json:"envs,omitempty"` // additional ENVs
}

// ToModel maps fixture to model.Branch.
func (b *Branch) ToModel(defaultBranch *model.Branch) *model.Branch {
	if b.IsDefault {
		return defaultBranch
	}

	branch := &model.Branch{}
	branch.Name = b.Name
	branch.Description = b.Description
	branch.IsDefault = b.IsDefault
	return branch
}

// ToModel maps fixture to model.Config.
func (c *Config) ToModel() *model.ConfigWithRows {
	config := &model.ConfigWithRows{Config: &model.Config{}}
	config.ComponentId = c.ComponentId
	config.Name = c.Name
	config.Description = "test fixture"
	config.ChangeDescription = "created by test"
	config.Content = c.Content

	for _, r := range c.Rows {
		row := r.ToModel()
		config.Rows = append(config.Rows, row)
	}

	return config
}

// ToModel maps fixture to model.Config.
func (r *ConfigRow) ToModel() *model.ConfigRow {
	row := &model.ConfigRow{}
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

func (b *Branch) ObjectName() string {
	return b.Name
}

func (c *Config) ObjectName() string {
	return c.Name
}

func (r *ConfigRow) ObjectName() string {
	return r.Name
}

func LoadStateFile(path string) (*StateFile, error) {
	data, err := os.ReadFile(path) // nolint: forbidigo
	if err != nil {
		return nil, fmt.Errorf(`cannot load test project state file "%s": %w`, path, err)
	}

	stateFile := &StateFile{}
	if err := json.Unmarshal(data, stateFile); err != nil {
		return nil, fmt.Errorf("cannot parse test project state file \"%s\": %w", path, err)
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
func LoadConfig(t *testing.T, name string) *model.ConfigWithRows {
	t.Helper()

	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)

	// Load file
	path := filesystem.Join(testDir, "configs", name+".json")
	data, err := os.ReadFile(path) // nolint: forbidigo
	if err != nil {
		panic(fmt.Errorf(`cannot load test confg file "%s": %w`, path, err))
	}

	// Parse file
	fixture := &Config{}
	if err := json.Unmarshal(data, fixture); err != nil {
		panic(fmt.Errorf("cannot parse test config file \"%s\": %w", path, err))
	}

	return fixture.ToModel()
}
