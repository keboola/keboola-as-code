package config

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	createConfig "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/config"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestAskCreateConfig(t *testing.T) {
	t.Parallel()

	d, console := dialog.NewForTest(t, true)

	fs := aferofs.NewMemoryFs()
	deps := dependencies.NewMocked(t, t.Context())
	ctx := t.Context()

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
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, 123, `foo.bar.com`),
	)))

	// Create branch files
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.MetaFile), `{"name": "Main"}`)))
	require.NoError(t, fs.WriteFile(ctx, filesystem.NewRawFile(filesystem.Join(`main`, naming.DescriptionFile), ``)))

	// Load project
	projectState, err := deps.MockedProject(fs).LoadState(ctx, loadState.Options{LoadLocalState: true}, deps)
	require.NoError(t, err)

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		require.NoError(t, console.ExpectString("Select the target branch"))

		require.NoError(t, console.SendEnter()) // enter - Main

		require.NoError(t, console.ExpectString("Select the target component"))

		require.NoError(t, console.SendLine("extractor generic\n"))

		require.NoError(t, console.ExpectString("Enter a name for the new config"))

		require.NoError(t, console.SendLine(`Foo Bar`))

		require.NoError(t, console.ExpectEOF())
	}()

	// Run
	opts, err := AskCreateConfig(projectState, d, deps, Flags{})
	require.NoError(t, err)
	require.NoError(t, console.Tty().Close())
	wg.Wait()
	require.NoError(t, console.Close())

	// Assert
	assert.Equal(t, createConfig.Options{
		BranchID:    123,
		ComponentID: `ex-generic-v2`,
		Name:        `Foo Bar`,
	}, opts)
}
