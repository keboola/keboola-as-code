package dialog_test

import (
	"context"
	"sync"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func TestDialogs_AskHostAndToken(t *testing.T) {
	t.Parallel()

	// testDependencies
	dialog, console := createDialogs(t, true)
	opts := options.New()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Please enter Keboola Storage API host, eg. \"connection.keboola.com\".")
		assert.NoError(t, err)

		_, err = console.ExpectString("API host: ")
		assert.NoError(t, err)

		_, err = console.SendLine(`foo.bar.com`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)

		_, err = console.ExpectString("API token: ")
		assert.NoError(t, err)

		_, err = console.SendLine(`my-secret-token`)
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	err := dialog.AskHostAndToken(opts)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, `foo.bar.com`, opts.Get(options.StorageApiHostOpt))
	assert.Equal(t, `my-secret-token`, opts.Get(options.StorageApiTokenOpt))
}

func TestDialogs_AskInitOptions(t *testing.T) {
	t.Parallel()

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()

	branches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	d.MockedHttpTransport().RegisterResponder(
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

		_, err := console.ExpectString("Allowed project's branches:")
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter, first option "only main branch"

		_, err = console.ExpectString(`Generate workflows files for GitHub Actions?`)
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - yes

		_, err = console.ExpectString(`Generate "validate" workflow?`)
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - yes

		_, err = console.ExpectString(`Generate "push" workflow?`)
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - yes

		_, err = console.ExpectString(`Generate "pull" workflow?`)
		assert.NoError(t, err)

		_, err = console.SendLine(`n`) // no
		assert.NoError(t, err)

		_, err = console.ExpectString(`Please select the main GitHub branch name:`)
		assert.NoError(t, err)

		assert.NoError(t, console.SendEnter()) // enter - main

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
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
