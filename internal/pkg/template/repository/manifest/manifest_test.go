package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

type test struct {
	name string
	json string
	data *Content
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
	manifest := newTestManifest(t)
	assert.NotNil(t, manifest)
}

func TestManifestLoadNotFound(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()

	// Load
	manifest, err := Load(fs, log.NewNopLogger())
	assert.Nil(t, manifest)
	assert.Error(t, err)
	assert.Equal(t, `manifest ".keboola/repository.json" not found`, err.Error())
}

func TestManifestLoad(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		fs := testhelper.NewMemoryFs()

		// Write file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		assert.NoError(t, fs.WriteFile(filesystem.NewFile(path, c.json)))

		// Load
		manifest, err := Load(fs, log.NewNopLogger())
		assert.NotNil(t, manifest)
		assert.NoError(t, err)

		// Assert
		assert.Equal(t, c.data, manifest.Content, c.name)
	}
}

func TestManifestSave(t *testing.T) {
	t.Parallel()
	for _, c := range cases() {
		// Create
		m := newTestManifest(t)

		// Save
		assert.NoError(t, m.Save())

		// Load file
		path := filesystem.Join(filesystem.MetadataDir, FileName)
		file, err := m.fs.ReadFile(path, "")
		assert.NoError(t, err)
		assert.Equal(t, testhelper.EscapeWhitespaces(c.json), testhelper.EscapeWhitespaces(file.Content), c.name)
	}
}

func TestManifestValidateEmpty(t *testing.T) {
	t.Parallel()
	m := &Manifest{Content: &Content{}}
	err := m.validate()
	assert.NotNil(t, err)
	expected := `manifest is not valid:
  - key="version", value="0", failed "required" validation`
	assert.Equal(t, expected, err.Error())
}

func TestManifestValidateMinimal(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	m := newManifest(fs)
	m.Content = minimalStruct()
	assert.NoError(t, m.validate())
}

func TestManifestValidateFull(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	m := newManifest(fs)
	m.Content = fullStruct()
	assert.NoError(t, m.validate())
}

func TestManifestValidateBadVersion(t *testing.T) {
	t.Parallel()
	fs := testhelper.NewMemoryFs()
	m := newManifest(fs)
	m.Content = minimalStruct()
	m.Version = 123
	err := m.validate()
	assert.Error(t, err)
	expected := "manifest is not valid:\n  - key=\"version\", value=\"123\", failed \"max\" validation"
	assert.Equal(t, expected, err.Error())
}

func minimalJson() string {
	return `{
  "version": 2
}
`
}

func minimalStruct() *Content {
	return &Content{
		Version: 2,
	}
}

func fullJson() string {
	return `{
  "version": 2
}
`
}

func fullStruct() *Content {
	return &Content{
		Version: 2,
	}
}

func newTestManifest(t *testing.T) *Manifest {
	t.Helper()
	fs := testhelper.NewMemoryFs()
	manifest, err := NewManifest(fs)
	assert.NoError(t, err)
	return manifest
}
