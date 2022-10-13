package manifest

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
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
	Url  string `json:"url" validate:"required"`
}

func newFile() *file {
	return &file{
		Version:   build.MajorVersion,
		Templates: make([]TemplateRecord, 0),
	}
}

func loadFile(fs filesystem.Fs) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(path) {
		return nil, errors.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	content := newFile()
	if _, err := fs.FileLoader().ReadJsonFileTo(filesystem.NewFileDef(path).SetDescription("manifest"), content); err != nil {
		return nil, err
	}

	// Fill in parent paths and convert nil components to empty slice
	for i := range content.Templates {
		template := &content.Templates[i]
		template.AbsPath.SetParentPath(``)
		for j := range template.Versions {
			version := &template.Versions[j]
			version.AbsPath.SetParentPath(template.Path())
			if version.Components == nil {
				version.Components = make([]string, 0)
			}
		}
	}

	// Validate
	if err := content.validate(); err != nil {
		return nil, err
	}

	// Set new version
	content.Version = build.MajorVersion

	return content, nil
}

func saveFile(fs filesystem.Fs, manifestContent *file) error {
	// Validate
	err := manifestContent.validate()
	if err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(manifestContent, true)
	if err != nil {
		return errors.PrefixError(err, "cannot encode manifest")
	}
	file := filesystem.NewRawFile(Path(), content)
	if err := fs.WriteFile(file); err != nil {
		return err
	}

	return nil
}

func (f *file) validate() error {
	if err := validator.New().Validate(context.Background(), f); err != nil {
		return errors.PrefixError(err, "repository manifest is not valid")
	}
	return nil
}

func (f *file) records() []TemplateRecord {
	out := make([]TemplateRecord, len(f.Templates))
	copy(out, f.Templates)
	return out
}
