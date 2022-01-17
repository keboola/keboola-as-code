package dialog_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
)

func TestAskCreateBranch(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := testdeps.New()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Enter a name for the new branch")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`Foo Bar`)
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskCreateBranch(d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createBranch.Options{
		Name: `Foo Bar`,
		Pull: true,
	}, opts)
}

func TestAskCreateConfig(t *testing.T) {
	t.Parallel()

	// Fs
	fs := testfs.NewMemoryFs()

	// Create manifest file
	manifestContent := `
{
  "version": 1,
  "project": {"id": %d, "apiHost": "%s"},
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}"
  },
  "branches": [{"id": 123, "path": "main"}]
}
`
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := testdeps.New()
	d.SetFs(fs)
	d.UseMockedSchedulerApi()
	_, httpTransport := d.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)
	testapi.AddMockedApiIndex(httpTransport)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select the target branch")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the target component")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine("extractor generic\n")
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter a name for the new config")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`Foo Bar`)
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskCreateConfig(d, createConfig.LoadStateOptions())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createConfig.Options{
		BranchId:    123,
		ComponentId: `ex-generic-v2`,
		Name:        `Foo Bar`,
	}, opts)
}

func TestAskCreateRow(t *testing.T) {
	t.Parallel()

	// Fs
	fs := testfs.NewMemoryFs()

	// Create manifest file
	manifestContent := `
{
  "version": 1,
  "project": {"id": %d, "apiHost": "%s"},
  "naming": {
    "branch": "{branch_name}",
    "config": "{component_type}/{component_id}/{config_name}",
    "configRow": "rows/{config_row_name}",
    "sharedCodeConfig": "_shared/{target_component_id}",
    "sharedCodeConfigRow": "codes/{config_row_name}"
  },
  "branches": [{"id": 123, "path": "main"}],
  "configurations": [
    {
      "branchId": 123,
      "componentId": "keboola.ex-db-mysql",
      "id": "456",
      "path": "extractor/keboola.ex-db-mysql/my-config"
    }
  ]
}
`
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Create config files
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `my-config`)
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(configDir, naming.MetaFile), `{"name": "My Config"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(configDir, naming.ConfigFile), `{}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewFile(filesystem.Join(configDir, naming.DescriptionFile), ``)))

	// testDependencies
	dialog, console := createDialogs(t, true)
	d := testdeps.New()
	d.SetFs(fs)
	d.UseMockedSchedulerApi()
	_, httpTransport := d.UseMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)
	testapi.AddMockedApiIndex(httpTransport)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		_, err := console.ExpectString("Select the target branch")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - My Config
		assert.NoError(t, err)

		_, err = console.ExpectString("Select the target config")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.Send(testhelper.Enter) // enter - My Config
		assert.NoError(t, err)

		_, err = console.ExpectString("Enter a name for the new config row")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = console.SendLine(`Foo Bar`)
		assert.NoError(t, err)

		_, err = console.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run
	opts, err := dialog.AskCreateRow(d, createRow.LoadStateOptions())
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createRow.Options{
		BranchId:    123,
		ComponentId: `keboola.ex-db-mysql`,
		ConfigId:    `456`,
		Name:        `Foo Bar`,
	}, opts)
}
