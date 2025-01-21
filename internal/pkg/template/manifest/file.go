package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	FileName = "manifest.jsonnet"
)

func Path() string {
	return filesystem.Join("src", FileName)
}

// file is template manifest JSON file.
type file struct {
	MainConfig *model.ConfigKey                `json:"mainConfig,omitempty"`
	Configs    []*model.ConfigManifestWithRows `json:"configurations" validate:"dive"`
}

func newFile() *file {
	return &file{
		Configs: make([]*model.ConfigManifestWithRows, 0),
	}
}

func evaluateFile(ctx context.Context, file *filesystem.RawFile, jsonnetCtx *jsonnet.Context) (*file, error) {
	// Evaluate Jsonnet code
	jsonContent, err := jsonnet.Evaluate(file.Content, jsonnetCtx)
	if err != nil {
		return nil, err
	}

	content := newFile()
	// Decode string and set Configs object
	if err := json.DecodeString(jsonContent, content); err != nil {
		return nil, err
	}

	// Validate
	if err := content.validate(ctx); err != nil {
		return nil, err
	}

	return content, nil
}

func saveFile(ctx context.Context, fs filesystem.Fs, content *file) error {
	// Validate
	if err := content.validate(ctx); err != nil {
		return err
	}

	// Convert to Json
	jsonContent, err := json.EncodeString(content, true)
	if err != nil {
		return err
	}

	// Convert to Jsonnet
	jsonnetStr, err := jsonnet.Format(jsonContent)
	if err != nil {
		return err
	}

	// Write file
	f := filesystem.NewRawFile(Path(), jsonnetStr)
	if err := fs.WriteFile(ctx, f); err != nil {
		return err
	}

	return nil
}

func (f *file) validate(ctx context.Context) error {
	ctx = context.WithValue(ctx, validator.DisableRequiredInProjectKey, true)
	if err := validator.New().ValidateCtx(ctx, f, "dive", ""); err != nil {
		return errors.PrefixError(err, "manifest is not valid")
	}
	return nil
}

func (f *file) records() ([]model.ObjectManifest, error) {
	out := make([]model.ObjectManifest, 0, len(f.Configs))
	for _, config := range f.Configs {
		if config == nil {
			continue
		}

		out = append(out, &config.ConfigManifest)
		for _, row := range config.Rows {
			row.ComponentID = config.ComponentID
			row.ConfigID = config.ID
			out = append(out, row)
		}
	}
	if len(out) == 0 {
		return nil, errors.New("unable to create template using invalid manifest configuration")
	}

	return out, nil
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
			panic(errors.New(`found unexpected BranchManifest in template manifest`))
		case *model.ConfigManifest:
			config := &model.ConfigManifestWithRows{
				ConfigManifest: *v,
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
			panic(errors.Errorf(`unexpected type "%T"`, manifest))
		}
	}
}
