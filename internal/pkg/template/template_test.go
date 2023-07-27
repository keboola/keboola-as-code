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
		Path:        "v1",
	}
	tmplRec := repository.TemplateRecord{
		ID:          tmplRef.TemplateID(),
		Name:        "Template 1",
		Description: "",
		Path:        "tmpl1",
		Versions:    []repository.VersionRecord{versionRec},
	}
	return &Template{_reference: tmplRef, template: tmplRec, version: versionRec, fs: fs}
}

func TestTemplate_TestsDir(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/two"))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.TestsDir()
	assert.NoError(t, err)
	assert.True(t, res.IsDir("one"))
	assert.True(t, res.IsDir("two"))
}

func TestTemplate_Test(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/one/sub1"))
	assert.NoError(t, fs.Mkdir("tests/two"))
	assert.NoError(t, fs.Mkdir("tests/two/sub2"))

	tmpl := initTemplate(t, fs)

	test, err := tmpl.Test("one")
	assert.NoError(t, err)
	assert.True(t, test.fs.IsDir("sub1"))
}

func TestTemplate_Tests(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.Mkdir("tests/two"))

	tmpl := initTemplate(t, fs)

	tests, err := tmpl.Tests()
	assert.NoError(t, err)

	testNames := make([]string, 0)
	for _, test := range tests {
		testNames = append(testNames, test.Name())
	}

	assert.Equal(t, []string{"one", "two"}, testNames)
}

func TestTemplate_TestInputs(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	assert.NoError(t, fs.Mkdir("tests/one"))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile("tests/one/inputs.json", `{"foo":"bar"}`)))

	tmpl := initTemplate(t, fs)

	test, err := tmpl.Test("one")
	assert.NoError(t, err)
	res, err := test.Inputs(nil, nil, "")
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{"foo": "bar"}, res)
}
