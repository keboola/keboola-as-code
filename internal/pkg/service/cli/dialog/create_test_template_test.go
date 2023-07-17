package dialog_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt/interactive"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	createOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/test/create"
)

func TestAskCreateTemplateTestInteractive(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, o, console := createDialogs(t, true)
	d := dependencies.NewMocked(t)
	addMockedObjectsResponses(d.MockedHTTPTransport())

	// Set fake file editor
	dialog.Prompt.(*interactive.Prompt).SetEditor(`true`)

	// Prepare the template
	fs, err := fixtures.LoadFS("template-simple", env.Empty())
	assert.NoError(t, err)
	version, err := model.NewSemVersion("v0.0.1")
	assert.NoError(t, err)
	tmplRef := model.NewTemplateRef(model.TemplateRepository{Name: "my-repository"}, "tmpl1", version.String())
	versionRec := repository.VersionRecord{
		Version:     version,
		Description: "",
		Stable:      true,
		Components:  []string{},
		AbsPath:     model.NewAbsPath("", "v1"),
	}
	tmplRec := repository.TemplateRecord{
		ID:          tmplRef.TemplateID(),
		Name:        "Template 1",
		Description: "",
		AbsPath:     model.NewAbsPath("", "tmpl1"),
		Versions:    []repository.VersionRecord{versionRec},
	}

	tmpl, err := template.New(context.Background(), tmplRef, tmplRec, versionRec, fs, fs, testapi.MockedComponentsMap())
	assert.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Default Group"))

		assert.NoError(t, console.ExpectString("Default Step"))

		assert.NoError(t, console.ExpectString("Default Step Description"))

		assert.NoError(t, console.ExpectString("url description"))

		assert.NoError(t, console.ExpectString("API URL:"))

		assert.NoError(t, console.SendLine(`foo.bar.com`))

		assert.NoError(t, console.ExpectString(`Enter the name of the environment variable that will fill input "API Token". Note that it will get prefix KBC_SECRET_.`))

		assert.NoError(t, console.ExpectString("API Token:"))

		assert.NoError(t, console.SendLine(`VAR1`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	o.Set(`test-name`, `one`)
	opts, warnings, err := dialog.AskCreateTemplateTestOptions(tmpl)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

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
