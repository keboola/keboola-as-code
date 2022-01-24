package manifest

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "manifest.jsonnet"
)

func Path() string {
	return FileName
}

// file is template manifest JSON file.
type file struct {
	Naming  naming.Template                 `json:"naming" validate:"required"`
	Configs []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func newFile() *file {
	return &file{
		Naming:  naming.ForTemplate(),
		Configs: make([]*model.ConfigManifestWithRows, 0),
	}
}

func loadFile(fs filesystem.Fs) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read file
	f, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("manifest"))
	if err != nil {
		return nil, err
	}

	// Decode JsonNet
	jsonContent, err := jsonnet.Evaluate(f.Content)
	if err != nil {
		return nil, err
	}

	// Decode Json
	content := newFile()
	if err := json.DecodeString(jsonContent, content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(); err != nil {
		return nil, err
	}

	return content, nil
}

func saveFile(fs filesystem.Fs, content *file) error {
	// Validate
	if err := content.validate(); err != nil {
		return err
	}

	// Convert to Json
	jsonContent, err := json.EncodeString(content, true)
	if err != nil {
		return err
	}

	// Convert to JsonNet
	jsonNet, err := jsonnet.Format(jsonContent)
	if err != nil {
		return err
	}

	// Write file
	f := filesystem.NewRawFile(Path(), jsonNet)
	if err := fs.WriteFile(f); err != nil {
		return err
	}

	return nil
}

func (f *file) validate() error {
	ctx := context.WithValue(context.Background(), validator.DisableRequiredInProjectKey, true)
	if err := validator.ValidateCtx(ctx, f, "dive", ""); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}

func (f *file) records() []model.ObjectManifest {
	var out []model.ObjectManifest
	for _, config := range f.Configs {
		out = append(out, config.ConfigManifest)
		for _, row := range config.Rows {
			row.ComponentId = config.ComponentId
			row.ConfigId = config.Id
			out = append(out, row)
		}
	}
	return out
}

func (f *file) setRecords(records []model.ObjectManifest) {
	// Convert records map to slices
	configsMap := make(map[string]*model.ConfigManifestWithRows)
	f.Configs = make([]*model.ConfigManifestWithRows, 0)

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
			panic(fmt.Errorf(`found unexpected BranchManifest in template manifest`))
		case *model.ConfigManifest:
			config := &model.ConfigManifestWithRows{
				ConfigManifest: v,
				Rows:           make([]*model.ConfigRowManifest, 0),
			}
			configsMap[config.String()] = config
			f.Configs = append(f.Configs, config)
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
