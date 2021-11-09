package cli

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Netflix/go-expect"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestInteractiveCreateConfig(t *testing.T) {
	t.Parallel()
	// Create virtual console
	c, state, err := testhelper.NewVirtualTerminal(t, expect.WithStdout(testhelper.VerboseStdout()), expect.WithDefaultTimeout(15*time.Second))
	assert.NoError(t, err)

	// Test project
	project := testproject.GetTestProject(t, env.Empty())
	project.SetState(`empty.json`)

	// Init prompt and cmd
	root := newTestRootCommandWithTty(c.Tty())
	root.cmd.SetArgs([]string{"create", "--storage-api-token", project.Token()})

	// Create fs
	logger, _ := utils.NewDebugLogger()
	fs, err := root.fsFactory(logger, `/`)
	assert.NoError(t, err)
	root.fsFactory = func(_ *zap.SugaredLogger, _ string) (filesystem.Fs, error) {
		return fs, nil
	}

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
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, project.Id(), project.StorageApiHost()),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`main`, model.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`main`, model.DescriptionFile), ``)))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = c.ExpectString("What do you want to create?")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.DownArrow) // select config
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - config
		assert.NoError(t, err)

		_, err = c.ExpectString("Enter a name for the new config")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(`test`)
		assert.NoError(t, err)

		_, err = c.ExpectString("Select the target branch")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - Main
		assert.NoError(t, err)

		_, err = c.ExpectString("Select the target component")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine("extractor generic\n")
		assert.NoError(t, err)

		_, err = c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run cmd
	assert.NoError(t, root.cmd.Execute())
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, `Created new config "main/extractor/ex-generic-v2/test"`)
}

func TestInteractiveCreateConfigRow(t *testing.T) {
	t.Parallel()

	// Create virtual console
	c, state, err := testhelper.NewVirtualTerminal(t, expect.WithStdout(testhelper.VerboseStdout()), expect.WithDefaultTimeout(15*time.Second))
	assert.NoError(t, err)

	// Test project
	project := testproject.GetTestProject(t, env.Empty())
	project.SetState(`empty.json`)

	// Init prompt and cmd
	root := newTestRootCommandWithTty(c.Tty())
	root.cmd.SetArgs([]string{"create", "--storage-api-token", project.Token()})

	// Create fs
	logger, _ := utils.NewDebugLogger()
	fs, err := root.fsFactory(logger, `/`)
	assert.NoError(t, err)
	root.fsFactory = func(_ *zap.SugaredLogger, _ string) (filesystem.Fs, error) {
		return fs, nil
	}

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
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(
		filesystem.Join(filesystem.MetadataDir, manifest.FileName),
		fmt.Sprintf(manifestContent, project.Id(), project.StorageApiHost()),
	)))

	// Create branch files
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`main`, model.MetaFile), `{"name": "Main"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(`main`, model.DescriptionFile), ``)))

	// Create config files
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `my-config`)
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, model.MetaFile), `{"name": "My Config"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, model.ConfigFile), `{}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, model.DescriptionFile), ``)))

	// Interaction
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err = c.ExpectString("What do you want to create?")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.DownArrow) // select config row
		assert.NoError(t, err)
		_, err = c.Send(testhelper.DownArrow) // select config row
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - config row
		assert.NoError(t, err)

		_, err = c.ExpectString("Enter a name for the new config row")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.SendLine(`test`)
		assert.NoError(t, err)

		_, err = c.ExpectString("Select the target branch")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - My Config
		assert.NoError(t, err)

		_, err = c.ExpectString("Select the target config")
		assert.NoError(t, err)

		time.Sleep(20 * time.Millisecond)
		_, err = c.Send(testhelper.Enter) // enter - My Config
		assert.NoError(t, err)

		_, err = c.ExpectEOF()
		assert.NoError(t, err)
	}()

	// Run cmd
	assert.NoError(t, root.cmd.Execute())
	assert.NoError(t, c.Tty().Close())
	wg.Wait()
	assert.NoError(t, c.Close())

	// Assert output
	out := expect.StripTrailingEmptyLines(state.String())
	assert.Contains(t, out, `Created new config row "main/extractor/keboola.ex-db-mysql/my-config/rows/test"`)
}
