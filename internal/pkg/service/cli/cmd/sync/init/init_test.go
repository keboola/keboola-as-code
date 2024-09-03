package init

import (
	"context"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func TestDialogs_AskInitOptions(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, context.Background())

	branches := []*model.Branch{{BranchKey: model.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}
	deps.MockedHTTPTransport().RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Allowed project's branches:"))

		assert.NoError(t, console.SendEnter()) // enter, first option "only main branch"

		assert.NoError(t, console.ExpectString(`Generate workflows files for GitHub Actions?`))

		assert.NoError(t, console.SendEnter()) // enter - yes

		assert.NoError(t, console.ExpectString(`Generate "validate" workflow?`))

		assert.NoError(t, console.SendEnter()) // enter - yes

		assert.NoError(t, console.ExpectString(`Generate "push" workflow?`))

		assert.NoError(t, console.SendEnter()) // enter - yes

		assert.NoError(t, console.ExpectString(`Generate "pull" workflow?`))

		assert.NoError(t, console.SendLine(`n`))

		assert.NoError(t, console.ExpectString(`Please select the main GitHub branch name:`))

		assert.NoError(t, console.SendEnter()) // enter - main

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskInitOptions(context.Background(), d, deps, DefaultFlags())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming:          naming.TemplateWithoutIds(),
			AllowedBranches: model.AllowedBranches{model.MainBranchDef},
		},
		Workflows: genWorkflows.Options{
			Validate:   true,
			Push:       true,
			Pull:       false,
			MainBranch: `main`,
		},
	}, opts)
}

func TestDialogs_AskInitOptions_No_CI(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	deps := dependencies.NewMocked(t, context.Background())

	branches := []*model.Branch{{BranchKey: model.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}
	deps.MockedHTTPTransport().RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)

	f := DefaultFlags()
	f.Branches = configmap.NewValueWithOrigin("main", configmap.SetByFlag)
	f.CI = configmap.NewValueWithOrigin(false, configmap.SetByFlag)

	// Run
	opts, err := AskInitOptions(context.Background(), d, deps, f)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, initOp.Options{
		Pull: true,
		ManifestOptions: createManifest.Options{
			Naming:          naming.TemplateWithoutIds(),
			AllowedBranches: model.AllowedBranches{model.MainBranchDef},
		},
		Workflows: genWorkflows.Options{MainBranch: "main"},
	}, opts)
}
