package manifest

import (
	"strings"
	"testing"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
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
			json: minimalJSON(),
			data: minimalStruct(),
		},
		{
			name: `full`,
			json: fullJSON(),
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
	fs := aferofs.NewMemoryFs()

	// Load
	manifest, err := Load(fs)
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/repository.json" not found`, err.Error())
}

func TestLoadManifestFile(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := aferofs.NewMemoryFs()

		// Write file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, c.json)))

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
		fs := aferofs.NewMemoryFs()

		// Save
		assert.NoError(t, saveFile(fs, c.data))

		// Load file
		file, err := fs.ReadFile(filesystem.NewFileDef(Path()))
		assert.NoError(t, err)
		assert.Equal(t, wildcards.EscapeWhitespaces(c.json), wildcards.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestContentValidateEmpty(t *testing.T) {
	t.Parallel()
	c := &file{}
	err := c.validate()
	assert.NotNil(t, err)
	expected := "repository manifest is not valid:\n- \"version\" is a required field\n- \"author.name\" is a required field\n- \"author.url\" is a required field"
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
	expected := `
repository manifest is not valid:
- "version" must be 2 or less
`
	assert.Equal(t, strings.TrimSpace(expected), err.Error())
}

func TestManifestBadRecordSemanticVersion(t *testing.T) {
	t.Parallel()
	fs := aferofs.NewMemoryFs()

	fileContent := `
{
  "version": 2,
  "templates": [
    {
      "id": "template-1",
      "name": "Template 1",
      "description": "My Template 1",
      "path": "template-1",
      "versions": [
        {
          "version": "foo-bar",
          "description": "SemVersion Bad",
          "stable": false,
          "path": "v0"
        }
      ]
    }
  ]
}
`

	// Write file
	path := filesystem.Join(filesystem.MetadataDir, FileName)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(path, fileContent)))

	// Load
	_, err := loadFile(fs)
	assert.Error(t, err)
	assert.Equal(t, "manifest file \".keboola/repository.json\" is invalid:\n- invalid semantic version \"foo-bar\"", err.Error())
}

func TestManifest_Records(t *testing.T) {
	t.Parallel()
	m := New()
	assert.Len(t, m.records, 0)

	// Get - not found
	v, found := m.GetByID("foo-bar")
	assert.Empty(t, v)
	assert.False(t, found)

	// GetOrCreate if record does not exist
	v = m.GetOrCreate("foo-bar")
	assert.NotEmpty(t, v)
	assert.Equal(t, "foo-bar", v.ID)
	assert.Equal(t, "foo-bar", v.Path())

	// Persist
	m.Persist(v)

	// Get - found
	v2, found := m.GetByID("foo-bar")
	assert.Equal(t, v, v2)
	assert.True(t, found)

	// GetOrCreate if record exists
	v3 := m.GetOrCreate("foo-bar")
	assert.Equal(t, v, v3)

	// Get all records, sorted by ID
	m.Persist(m.GetOrCreate("xyz"))
	m.Persist(m.GetOrCreate("abc"))
	assert.Equal(t, []TemplateRecord{
		{
			ID:      "abc",
			AbsPath: model.NewAbsPath("", "abc"),
		},
		{
			ID:      "foo-bar",
			AbsPath: model.NewAbsPath("", "foo-bar"),
		},
		{
			ID:      "xyz",
			AbsPath: model.NewAbsPath("", "xyz"),
		},
	}, m.AllTemplates())
}

func TestManifest_GetByPath_NotFound(t *testing.T) {
	t.Parallel()
	m := New()
	record, found := m.GetByPath(`foo`)
	assert.Empty(t, record)
	assert.False(t, found)
}

func TestManifest_GetByPath_Found(t *testing.T) {
	t.Parallel()
	m := New()
	record1 := TemplateRecord{ID: "foo", AbsPath: model.NewAbsPath("parent", "foo")}
	m.Persist(record1)
	record2 := TemplateRecord{ID: "bar", AbsPath: model.NewAbsPath("parent", "bar")}
	m.Persist(record2)
	record, found := m.GetByPath(`foo`)
	assert.Equal(t, record1, record)
	assert.True(t, found)
}

func TestManifest_GetVersion(t *testing.T) {
	t.Parallel()
	m := New()
	record := TemplateRecord{ID: "foo", AbsPath: model.NewAbsPath("parent", "foo")}
	record.AddVersion(version("1.2.3"), []string{})
	m.Persist(record)

	// Version found
	_, v, err := m.GetVersion("foo", "v1")
	assert.NoError(t, err)
	assert.Equal(t, version("1.2.3"), v.Version)

	// Version not found
	_, _, err = m.GetVersion("foo", "v2")
	assert.Error(t, err)
	assert.Equal(t, `template "foo" found but version "v2" is missing`, err.Error())

	// Template not found
	_, _, err = m.GetVersion("bar", "v1")
	assert.Error(t, err)
	assert.Equal(t, `template "bar" not found`, err.Error())
}

func minimalJSON() string {
	return `{
  "version": 2,
  "author": {
    "name": "Author",
    "url": "https://example.com"
  },
  "templates": []
}
`
}

func minimalStruct() *file {
	return &file{
		Version: 2,
		Author: Author{
			Name: "Author",
			URL:  "https://example.com",
		},
		Templates: []TemplateRecord{},
	}
}

func fullJSON() string {
	return `{
  "version": 2,
  "author": {
    "name": "Author",
    "url": "https://example.com"
  },
  "templates": [
    {
      "id": "template-1",
      "name": "Template 1",
      "description": "My Template 1",
      "path": "template-1",
      "versions": [
        {
          "version": "0.0.1",
          "description": "SemVersion 0",
          "stable": false,
          "path": "v0"
        },
        {
          "version": "1.2.3",
          "description": "SemVersion 1",
          "stable": true,
          "components": [
            "foo",
            "bar"
          ],
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
		Author: Author{
			Name: "Author",
			URL:  "https://example.com",
		},
		Templates: []TemplateRecord{
			{
				AbsPath:     model.NewAbsPath(``, `template-1`),
				ID:          "template-1",
				Name:        `Template 1`,
				Description: `My Template 1`,
				Versions: []VersionRecord{
					{
						AbsPath:     model.NewAbsPath(`template-1`, `v0`),
						Version:     version(`0.0.1`),
						Stable:      false,
						Components:  []string{},
						Description: `SemVersion 0`,
					},
					{
						AbsPath:     model.NewAbsPath(`template-1`, `v1`),
						Version:     version(`1.2.3`),
						Stable:      true,
						Components:  []string{"foo", "bar"},
						Description: `SemVersion 1`,
					},
				},
			},
		},
	}
}

func version(str string) model.SemVersion {
	v, err := model.NewSemVersion(str)
	if err != nil {
		panic(err)
	}
	return v
}
