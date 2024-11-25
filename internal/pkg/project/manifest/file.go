package manifest

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	Version int     `json:"version" validate:"required,min=1,max=2"`
	Project Project `json:"project" validate:"required"`
	// AllowTargetENV allows usage KBC_PROJECT_ID and KBC_BRANCH_ID envs to override manifest values
	AllowTargetENV    bool                            `json:"allowTargetEnv"`
	SortBy            string                          `json:"sortBy" validate:"oneof=id path"`
	Naming            naming.Template                 `json:"naming" validate:"required"`
	AllowedBranches   model.AllowedBranches           `json:"allowedBranches" validate:"required,min=1"`
	IgnoredComponents model.ComponentIDs              `json:"ignoredComponents"`
	Templates         Templates                       `json:"templates,omitempty"`
	Branches          []*model.BranchManifest         `json:"branches" validate:"dive"`
	Configs           []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

type Templates struct {
	Repositories []model.TemplateRepository `json:"repositories,omitempty" validate:"dive"`
}

func newFile(projectID keboola.ProjectID, apiHost string) *file {
	return &file{
		Version:           build.MajorVersion,
		Project:           Project{ID: projectID, APIHost: apiHost},
		SortBy:            model.SortByID,
		Naming:            naming.TemplateWithIds(),
		AllowedBranches:   model.DefaultAllowedBranches(),
		IgnoredComponents: model.ComponentIDs{},
		Templates:         Templates{Repositories: []model.TemplateRepository{repository.DefaultRepository(), repository.ComponentsRepository()}},
		Branches:          make([]*model.BranchManifest, 0),
		Configs:           make([]*model.ConfigManifestWithRows, 0),
	}
}

func loadFile(ctx context.Context, fs filesystem.Fs) (*file, error) {
	path := Path()

	// Exists?
	if !fs.IsFile(ctx, path) {
		return nil, errors.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	content := newFile(0, "")
	if _, err := fs.FileLoader().ReadJSONFileTo(ctx, filesystem.NewFileDef(path).SetDescription("manifest"), content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(ctx); err != nil {
		return content, err
	}

	return content, nil
}

func saveFile(ctx context.Context, fs filesystem.Fs, f *file) error {
	// Validate
	if err := f.validate(ctx); err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(f, true)
	if err != nil {
		return errors.PrefixError(err, "cannot encode manifest")
	}
	file := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(ctx, file); err != nil {
		return err
	}
	return nil
}

func (c *file) validate(ctx context.Context) error {
	if err := validator.New().Validate(ctx, c); err != nil {
		return errors.PrefixError(err, "manifest is not valid")
	}
	return nil
}

func (c *file) records() []model.ObjectManifest {
	out := make([]model.ObjectManifest, 0, 1024)
	for _, branch := range c.Branches {
		out = append(out, branch)
	}
	for _, config := range c.Configs {
		out = append(out, &config.ConfigManifest)
		for _, row := range config.Rows {
			row.BranchID = config.BranchID
			row.ComponentID = config.ComponentID
			row.ConfigID = config.ID
			out = append(out, row)
		}
	}
	return out
}

func (c *file) setRecords(records []model.ObjectManifest) {
	// Convert records map to slices
	branchesMap := make(map[string]*model.BranchManifest)
	configsMap := make(map[string]*model.ConfigManifestWithRows)
	c.Branches = make([]*model.BranchManifest, 0, len(records))
	c.Configs = make([]*model.ConfigManifestWithRows, 0, len(records))

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
					ConfigManifest: *v,
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
			panic(errors.Errorf(`unexpected type "%T"`, manifest))
		}
	}
}
