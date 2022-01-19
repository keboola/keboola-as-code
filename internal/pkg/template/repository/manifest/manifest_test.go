package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

type test struct {
	name string
	json string
	data *file
}

func cases() []test {
	return []test{
		{
			name: `minimal`,
			json: minimalJson(),
			data: minimalStruct(),
		},
		{
			name: `full`,
			json: fullJson(),
			data: fullStruct(),
		},
	}
}

func TestNewManifest(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, New())
}

func TestManifestFileNotFound(t *testing.T) {
	t.Parallel()
	fs := testfs.NewMemoryFs()

	// Load
	manifest, err := Load(fs)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/repository.json" not found`, err.Error())
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Write file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		assert.NoError(t, fs.WriteFile(filesystem.NewFile(path, c.json)))

		// Load
		manifestContent, err := loadFile(fs)
		assert.NotNil(t, manifestContent)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.data, manifestContent, c.name)
	}
}

func TestSaveManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testfs.NewMemoryFs()

		// Save
		assert.NoError(t, saveFile(fs, c.data))

		// Load file
		file, err := fs.ReadFile(Path(), "")
		assert.NoError(t, err)
		assert.Equal(t, testhelper.EscapeWhitespaces(c.json), testhelper.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestContentValidateEmpty(t *testing.T) {
	t.Parallel()
	c := &file{}
	err := c.validate()
	assert.NotNil(t, err)
	expected := "repository manifest is not valid:\n  - version is a required field"
	assert.Equal(t, expected, err.Error())
}

func TestManifestContentValidateMinimal(t *testing.T) {
	t.Parallel()
	assert.NoError(t, minimalStruct().validate())
}

func TestManifestContentValidateFull(t *testing.T) {
	t.Parallel()
	assert.NoError(t, fullStruct().validate())
}

func TestManifestContentValidateBadVersion(t *testing.T) {
	t.Parallel()
	manifestContent := minimalStruct()
	manifestContent.Version = 123
	err := manifestContent.validate()
	assert.Error(t, err)
	expected := "repository manifest is not valid: version must be 2 or less"
	assert.Equal(t, expected, err.Error())
}

func minimalJson() string {
	return `{
  "version": 2,
  "templates": []
}
`
}

func minimalStruct() *file {
	return &file{
		Version:   2,
		Templates: []*TemplateRecord{},
	}
}

func fullJson() string {
	return `{
  "version": 2,
  "templates": [
    {
      "id": "template-1",
      "name": "Template 1",
      "description": "My Template 1",
      "path": "template-1",
      "versions": [
        {
          "version": "0.0.1",
          "description": "Version 0",
          "stable": false,
          "path": "v0"
        },
        {
          "version": "1.2.3",
          "description": "Version 1",
          "stable": true,
          "path": "v1"
        }
      ]
    }
  ]
}
`
}

func fullStruct() *file {
	return &file{
		Version: 2,
		Templates: []*TemplateRecord{
			{
				AbsPath:     model.NewAbsPath(``, `template-1`),
				Id:          "template-1",
				Name:        `Template 1`,
				Description: `My Template 1`,
				Versions: []*VersionRecord{
					{
						AbsPath:     model.NewAbsPath(`template-1`, `v0`),
						Version:     `0.0.1`,
						Stable:      false,
						Description: `Version 0`,
					},
					{
						AbsPath:     model.NewAbsPath(`template-1`, `v1`),
						Version:     `1.2.3`,
						Stable:      true,
						Description: `Version 1`,
					},
				},
			},
		},
	}
}
