package dialog_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func TestDialogs_AskHostAndToken(t *testing.T) {
	t.Parallel()

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()
	opts := d.Options()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Please enter Keboola Storage API host, eg. \"connection.keboola.com\"."))

		assert.NoError(t, console.ExpectString("API host: "))

		assert.NoError(t, console.SendLine(`foo.bar.com`))

		assert.NoError(t, console.ExpectString("Please enter Keboola Storage API token. The value will be hidden."))

		assert.NoError(t, console.ExpectString("API token: "))

		assert.NoError(t, console.SendLine(`my-secret-token`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	err := dialog.AskHostAndToken(d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, `foo.bar.com`, opts.Get(options.StorageAPIHostOpt))
	assert.Equal(t, `my-secret-token`, opts.Get(options.StorageAPITokenOpt))
}

func TestDialogs_AskInitOptions(t *testing.T) {
	t.Parallel()

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()

	branches := []*model.Branch{{BranchKey: model.BranchKey{ID: 123}, Name: "Main", IsDefault: true}}
	d.MockedHTTPTransport().RegisterResponder(
		"GET", `=~/storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, branches),
	)

	// Default values are defined by options
	flags := pflag.NewFlagSet(``, pflag.ExitOnError)
	ci.WorkflowsCmdFlags(flags)
	assert.NoError(t, d.Options().BindPFlags(flags))

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
	opts, err := dialog.AskInitOptions(context.Background(), d)
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
