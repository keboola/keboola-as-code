package manifest

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "manifest.json"
)

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

// file is template repository manifest JSON file.
type file struct {
	Version           int                             `json:"version" validate:"required,min=1,max=2"`
	Project           Project                         `json:"project" validate:"required"`
	SortBy            string                          `json:"sortBy" validate:"oneof=id path"`
	Naming            naming.Template                 `json:"naming" validate:"required"`
	AllowedBranches   model.AllowedBranches           `json:"allowedBranches" validate:"required,min=1"`
	IgnoredComponents model.ComponentIds              `json:"ignoredComponents"`
	Branches          []*model.BranchManifest         `json:"branches" validate:"dive"`
	Configs           []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func newFile(projectId int, apiHost string) *file {
	return &file{
		Version:           build.MajorVersion,
		Project:           Project{Id: projectId, ApiHost: apiHost},
		SortBy:            model.SortById,
		Naming:            naming.TemplateWithIds(),
		AllowedBranches:   model.DefaultAllowedBranches(),
		IgnoredComponents: model.ComponentIds{},
		Branches:          make([]*model.BranchManifest, 0),
		Configs:           make([]*model.ConfigManifestWithRows, 0),
	}
}

func loadFile(fs filesystem.Fs) (*file, error) {
	path := Path()

	// Exists?
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	content := newFile(0, "")
	if _, err := fileloader.New(fs).ReadJsonFileTo(filesystem.NewFileDef(path).SetDescription("manifest"), content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(); err != nil {
		return nil, err
	}

	return content, nil
}

func saveFile(fs filesystem.Fs, f *file) error {
	// Validate
	if err := f.validate(); err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(f, true)
	if err != nil {
		return utils.PrefixError(`cannot encode manifest`, err)
	}
	file := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(file); err != nil {
		return err
	}
	return nil
}

func (c *file) validate() error {
	if err := validator.Validate(context.Background(), c); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}

func (c *file) records() []model.ObjectManifest {
	var out []model.ObjectManifest
	for _, branch := range c.Branches {
		out = append(out, branch)
	}
	for _, config := range c.Configs {
		out = append(out, config.ConfigManifest)
		for _, row := range config.Rows {
			row.BranchId = config.BranchId
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			out = append(out, row)
		}
	}
	return out
}

func (c *file) setRecords(records []model.ObjectManifest) {
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
