package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

// Content of the project directory manifest.
// Content contains IDs and paths of the all objects: branches, configs, rows.
type Content struct {
	Version int           `json:"version" validate:"required,min=1,max=2"`
	Project model.Project `json:"project" validate:"required"`
	SortBy  string        `json:"sortBy" validate:"oneof=id path"`
	Naming  *model.Naming `json:"naming" validate:"required"`
	model.Filter
	Branches []*model.BranchManifest         `json:"branches" validate:"dive"`
	Configs  []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func newContent(projectId int, apiHost string) *Content {
	return &Content{
		Version:  build.MajorVersion,
		Project:  model.Project{Id: projectId, ApiHost: apiHost},
		SortBy:   model.SortById,
		Naming:   model.DefaultNamingWithIds(),
		Filter:   model.DefaultFilter(),
		Branches: make([]*model.BranchManifest, 0),
		Configs:  make([]*model.ConfigManifestWithRows, 0),
	}
}

func LoadContent(fs filesystem.Fs, path string) (*Content, error) {
	// Exists?
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	content := newContent(0, "")
	if err := fs.ReadJsonFileTo(path, "manifest", content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(); err != nil {
		return nil, err
	}

	return content, nil
}

func (c *Content) Save(fs filesystem.Fs, path string) error {
	// Validate
	err := c.validate()
	if err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(c, true)
	if err != nil {
		return utils.PrefixError(`cannot encode manifest`, err)
	}
	file := filesystem.NewFile(path, content)
	if err := fs.WriteFile(file); err != nil {
		return err
	}
	return nil
}

func (c *Content) SetRecords(records []model.ObjectManifest) {
	// Convert records map to slices
	branchesMap := make(map[string]*model.BranchManifest)
	configsMap := make(map[string]*model.ConfigManifestWithRows)
	c.Branches = make([]*model.BranchManifest, 0)
	c.Configs = make([]*model.ConfigManifestWithRows, 0)

	for _, manifest := range records {
		// Skip invalid (eg. missing config file)
		if manifest.State().IsInvalid() {
			continue
		}

		// Skip not persisted
		if !manifest.State().IsPersisted() {
			continue
		}

		// Generate content, we have to check if parent exists (eg. branch could have been deleted)
		switch v := manifest.(type) {
		case *model.BranchManifest:
			c.Branches = append(c.Branches, v)
			branchesMap[v.String()] = v
		case *model.ConfigManifest:
			_, found := branchesMap[v.BranchKey().String()]
			if found {
				config := &model.ConfigManifestWithRows{
					ConfigManifest: v,
					Rows:           make([]*model.ConfigRowManifest, 0),
				}
				configsMap[config.String()] = config
				c.Configs = append(c.Configs, config)
			}
		case *model.ConfigRowManifest:
			config, found := configsMap[v.ConfigKey().String()]
			if found {
				config.Rows = append(config.Rows, v)
			}
		default:
			panic(fmt.Errorf(`unexpected type "%T"`, manifest))
		}
	}
}

func (c *Content) validate() error {
	if err := validator.Validate(c); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}
