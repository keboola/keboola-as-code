package dialog_test

import (
	"sync"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/cmd/ci"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/init"
)

func TestAskInitOptions(t *testing.T) {
	t.Parallel()

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := dependencies.NewTestContainer()
	_, httpTransport := d.UseMockedStorageApi()

	branches := []*model.Branch{{BranchKey: model.BranchKey{Id: 123}, Name: "Main", IsDefault: true}}
	httpTransport.RegisterResponder(
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

		_, err := console.ExpectString("Please enter Keboola Storage API host, eg. \"connection.keboola.com\".")
		assert.NoError(t, err)

		_, err = console.ExpectString("API host: ")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`foo.bar.com`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Please enter Keboola Storage API token. The value will be hidden.")
		assert.NoError(t, err)

		_, err = console.ExpectString("API token: ")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`my-secret-token`)
		assert.NoError(t, err)

		_, err = console.ExpectString("Allowed project's branches:")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter, first option "only main branch"
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate workflows files for GitHub Actions?`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate "validate" workflow?`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate "push" workflow?`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - yes
		assert.NoError(t, err)

		_, err = console.ExpectString(`Generate "pull" workflow?`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`n`) // no
		assert.NoError(t, err)

		_, err = console.ExpectString(`Please select the main GitHub branch name:`)
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - main
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskInitOptions(d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, `foo.bar.com`, d.Options().Get(options.StorageApiHostOpt))
	assert.Equal(t, `my-secret-token`, d.Options().Get(options.StorageApiTokenOpt))
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
