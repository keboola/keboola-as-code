package template

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func initTemplate(t *testing.T, fs filesystem.Fs) *Template {
	t.Helper()

	version, err := model.NewSemVersion("v0.0.1")
	require.NoError(t, err)
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
	ctx := context.Background()

	require.NoError(t, fs.Mkdir(ctx, "tests/one"))
	require.NoError(t, fs.Mkdir(ctx, "tests/two"))

	tmpl := initTemplate(t, fs)

	res, err := tmpl.TestsDir(ctx)
	require.NoError(t, err)
	assert.True(t, res.IsDir(ctx, "one"))
	assert.True(t, res.IsDir(ctx, "two"))
}

func TestTemplate_Test(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	ctx := context.Background()

	require.NoError(t, fs.Mkdir(ctx, "tests/one"))
	require.NoError(t, fs.Mkdir(ctx, "tests/one/sub1"))
	require.NoError(t, fs.Mkdir(ctx, "tests/two"))
	require.NoError(t, fs.Mkdir(ctx, "tests/two/sub2"))

	tmpl := initTemplate(t, fs)

	test, err := tmpl.Test(ctx, "one")
	require.NoError(t, err)
	assert.True(t, test.fs.IsDir(ctx, "sub1"))
}

func TestTemplate_Tests(t *testing.T) {
	t.Parallel()

	logger := log.NewDebugLogger()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))
	ctx := context.Background()

	require.NoError(t, fs.Mkdir(ctx, "tests/one"))
	require.NoError(t, fs.Mkdir(ctx, "tests/two"))

	tmpl := initTemplate(t, fs)

	tests, err := tmpl.Tests(ctx)
	require.NoError(t, err)

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
	ctx := context.Background()

	require.NoError(t, fs.Mkdir(ctx, "tests/one"))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile("tests/one/inputs.json", `{"foo":"bar"}`)))

	tmpl := initTemplate(t, fs)

	test, err := tmpl.Test(ctx, "one")
	require.NoError(t, err)
	res, err := test.Inputs(ctx, nil, nil, "")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, res)
}
