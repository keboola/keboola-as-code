package row

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskCreateRow(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	fs := aferofs.NewMemoryFs()
	deps, _ := dependencies.NewMocked(t, context.Background())
	ctx := context.Background()

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
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Create config files
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `my-config`)
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.MetaFile), `{"name": "My Config"}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.ConfigFile), `{}`)))
	assert.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(configDir, naming.DescriptionFile), ``)))

	// Test dependencies
	projectState, err := deps.MockedProject(fs).LoadState(loadState.Options{LoadLocalState: true}, deps)
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
	opts, err := AskCreateRow(projectState, d, Flags{})
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
