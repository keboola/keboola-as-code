package template

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func initTemplate(t *testing.T, fs filesystem.Fs) *Template {
	t.Helper()

	version, err := model.NewSemVersion("v0.0.1")
	assert.NoError(t, err)
	tmplRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "my-template", version.String())
	versionRec := repository.VersionRecord{
		Version:     version,
		Description: "",
		Stable:      true,
		Components:  []string{},
		AbsPath:     model.NewAbsPath("", "v1"),
	}
	tmplRec := repository.TemplateRecord{
		Id:          tmplRef.TemplateId(),
		Name:        "Template 1",
		Description: "",
		AbsPath:     model.NewAbsPath("", "tmpl1"),
		Versions:    []repository.VersionRecord{versionRec},
	}
	return &Template{_reference: tmplRef, template: tmplRec, version: versionRec, fs: fs}
}

func TestTemplate_TestsDir(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/two"))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.TestsDir()
	assert.NoError(t, err)
	assert.True(t, res.IsDir("one"))
	assert.True(t, res.IsDir("two"))
}

func TestTemplate_TestDir(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/one/sub1"))
	assert.NoError(t, fs.Mkdir("tests/two"))
	assert.NoError(t, fs.Mkdir("tests/two/sub2"))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.TestDir("one")
	assert.NoError(t, err)
	assert.True(t, res.IsDir("sub1"))
}

func TestTemplate_ListTests(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/two"))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.ListTests()
	assert.NoError(t, err)
	assert.Equal(t, []string{"one", "two"}, res)
}

func TestTemplate_TestInputs(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("tests/one/inputs.json", `{"foo":"bar"}`)))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.TestInputs("one", nil, nil, "")
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"foo": "bar"}, res)
}
