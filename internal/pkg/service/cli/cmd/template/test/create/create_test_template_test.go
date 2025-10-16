package create

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog/templatehelper"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

func TestAskCreateTemplateTestInteractive(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, t.Context())

	templatehelper.AddMockedObjectsResponses(deps.MockedHTTPTransport())

	// Set fake file editor
	d.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Prepare the template
	fs, err := fixtures.LoadFS(t.Context(), "template-simple", env.Empty())
	require.NoError(t, err)
	version, err := model.NewSemVersion("v0.0.1")
	require.NoError(t, err)
	tmplRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "tmpl1", version.String())
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

	tmpl, err := template.New(t.Context(), tmplRef, tmplRec, versionRec, fs, fs, "", testapi.MockedComponentsMap())
	require.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Go(func() {

		require.NoError(t, console.ExpectString("Default Group"))

		require.NoError(t, console.ExpectString("Default Step"))

		require.NoError(t, console.ExpectString("Default Step Description"))

		require.NoError(t, console.ExpectString("url description"))

		require.NoError(t, console.ExpectString("API URL:"))

		require.NoError(t, console.SendLine(`foo.bar.com`))

		require.NoError(t, console.ExpectString(`Enter the name of the environment variable that will fill input "API Token". Note that it will get prefix KBC_SECRET_.`))

		require.NoError(t, console.ExpectString("API Token:"))

		require.NoError(t, console.SendLine(`VAR1`))

		require.NoError(t, console.ExpectEOF())
	})

	// Run
	f := Flags{
		TestName: configmap.NewValueWithOrigin("one", configmap.SetByFlag),
	}
	opts, warnings, err := AskCreateTemplateTestOptions(t.Context(), d, tmpl, f)
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createOp.Options{
		TestName: "one",
		Inputs: input.Values{
			{
				ID:      "generic-url",
				Value:   "foo.bar.com",
				Skipped: false,
			},
			{
				ID:      "generic-token",
				Value:   "##KBC_SECRET_VAR1##",
				Skipped: false,
			},
		},
	}, opts)
	assert.Equal(t, []string{`Input "API Token" expects setting of environment variable "KBC_SECRET_VAR1".`}, warnings)
}
