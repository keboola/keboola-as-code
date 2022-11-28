package dialog_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	createBranch "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/create/branch"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskCreateBranch(t *testing.T) {
	t.Parallel()
	dialog, console := createDialogs(t, true)
	d := dependencies.NewMockedDeps()

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Enter a name for the new branch"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
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

	// Test dependencies
	dialog, console := createDialogs(t, true)
	fs := aferofs.NewMemoryFs()
	d := dependencies.NewMockedDeps()

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
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Load project
	projectState, err := d.MockedProject(fs).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch"))

		assert.NoError(t, console.SendEnter()) // enter - Main

		assert.NoError(t, console.ExpectString("Select the target component"))

		assert.NoError(t, console.SendLine("extractor generic\n"))

		assert.NoError(t, console.ExpectString("Enter a name for the new config"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := dialog.AskCreateConfig(projectState, d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createConfig.Options{
		BranchID:    123,
		ComponentID: `ex-generic-v2`,
		Name:        `Foo Bar`,
	}, opts)
}

func TestAskCreateRow(t *testing.T) {
	t.Parallel()

	// Test dependencies
	dialog, console := createDialogs(t, true)
	fs := aferofs.NewMemoryFs()
	d := dependencies.NewMockedDeps()

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
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Create config files
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `my-config`)
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(configDir, naming.MetaFile), `{"name": "My Config"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(configDir, naming.ConfigFile), `{}`)))
	assert.NoError(t, fs.WriteFile(filesystem.NewRawFile(filesystem.Join(configDir, naming.DescriptionFile), ``)))

	// Test dependencies
	projectState, err := d.MockedProject(fs).LoadState(loadState.Options{LoadLocalState: true}, d)
	assert.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		assert.NoError(t, console.ExpectString("Select the target branch"))

		assert.NoError(t, console.SendEnter()) // enter - My Config

		assert.NoError(t, console.ExpectString("Select the target config"))

		assert.NoError(t, console.SendEnter()) // enter - My Config

		assert.NoError(t, console.ExpectString("Enter a name for the new config row"))

		assert.NoError(t, console.SendLine(`Foo Bar`))

		assert.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := dialog.AskCreateRow(projectState, d)
	assert.NoError(t, err)
	assert.NoError(t, console.Tty().Close())
	wg.Wait()
	assert.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createRow.Options{
		BranchID:    123,
		ComponentID: `keboola.ex-db-mysql`,
		ConfigID:    `456`,
		Name:        `Foo Bar`,
	}, opts)
}
