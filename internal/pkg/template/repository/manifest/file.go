package manifest

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const FileName = `repository.json`

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

// file is repository manifest JSON file.
type file struct {
	Version   int              `json:"version" validate:"required,min=1,max=2"`
	Author    Author           `json:"author" validate:"dive"`
	Templates []TemplateRecord `json:"templates" validate:"dive"`
}

type Author struct {
	Name string `json:"name" validate:"required"`
	URL  string `json:"url" validate:"required"`
}

func newFile() *file {
	return &file{
		Version:   build.MajorVersion,
		Templates: make([]TemplateRecord, 0),
	}
}

func loadFile(ctx context.Context, fs filesystem.Fs) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(ctx, path) {
		return nil, errors.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	content := newFile()
	if _, err := fs.FileLoader().ReadJSONFileTo(ctx, filesystem.NewFileDef(path).SetDescription("manifest"), content); err != nil {
		return nil, err
	}

	// Fill in parent paths and convert nil components to empty slice
	for i := range content.Templates {
		template := &content.Templates[i]
		for j := range template.Versions {
			version := &template.Versions[j]
			if version.Components == nil {
				version.Components = make([]string, 0)
			}
		}
	}

	// Validate
	if err := content.validate(ctx); err != nil {
		return nil, err
	}

	// Set new version
	content.Version = build.MajorVersion

	return content, nil
}

func saveFile(ctx context.Context, fs filesystem.Fs, manifestContent *file) error {
	// Validate
	err := manifestContent.validate(ctx)
	if err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(manifestContent, true)
	if err != nil {
		return errors.PrefixError(err, "cannot encode manifest")
	}
	file := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(ctx, file); err != nil {
		return err
	}

	return nil
}

func (f *file) validate(ctx context.Context) error {
	errs := errors.NewMultiError()
	if err := validator.New().Validate(ctx, f); err != nil {
		errs.Append(err)
	}

	// Validate path. It should be set for template and version, except deprecated templates.
	for i, t := range f.Templates {
		tPathEmpty := strings.TrimSpace(t.Path) == ""
		if tPathEmpty && !t.Deprecated {
			errs.Append(errors.Errorf(`"templates[%d].path" is a required field`, i))
		}
		if !tPathEmpty && t.Deprecated {
			errs.Append(errors.Errorf(`"templates[%d].path" is not expected for the deprecated template`, i))
		}
		for j, v := range t.Versions {
			vPathEmpty := strings.TrimSpace(v.Path) == ""
			if vPathEmpty && !t.Deprecated {
				errs.Append(errors.Errorf(`"templates[%d].version[%d].path" is a required field`, i, j))
			}
			if !vPathEmpty && t.Deprecated {
				errs.Append(errors.Errorf(`"templates[%d].version[%d].path" is not expected for the deprecated template`, i, j))
			}
		}
	}

	if errs.Len() > 0 {
		return errors.PrefixError(errs, "repository manifest is not valid")
	}

	return nil
}

func (f *file) records() []TemplateRecord {
	out := make([]TemplateRecord, len(f.Templates))
	copy(out, f.Templates)
	return out
}
