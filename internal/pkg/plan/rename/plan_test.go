package rename

import (
	"context"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRenameAllPlan(t *testing.T) {
	t.Parallel()
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	m, fs := loadTestManifest(t, filesystem.Join(testDir, "..", "..", "fixtures", "local", "to-rename"))

	// Load state
	logger, _ := utils.NewDebugLogger()
	api, httpTransport, _ := testapi.NewMockedStorageApi()

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic",
	})
	assert.NoError(t, err)
	getMySqlExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "keboola.ex-db-mysql",
		"type": "extractor",
		"name": "MySQL",
	})
	assert.NoError(t, err)
	httpTransport.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	httpTransport.RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySqlExResponder.Once())

	// Load state
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	options := state.NewOptions(fs, m, api, schedulerApi, context.Background(), logger)
	options.LoadLocalState = true
	projectState, ok, localErr, remoteErr := state.LoadState(options)
	assert.True(t, ok)
	assert.NoError(t, localErr)
	assert.NoError(t, remoteErr)

	// Get rename plan
	plan, err := NewPlan(projectState)
	assert.NoError(t, err)

	// Clear manifest records before assert
	for _, action := range plan.actions {
		action.Manifest = nil
	}

	// Clear manifest ObjectManifest from actions for easier comparison
	for i := range plan.actions {
		plan.actions[i].Manifest = nil
	}

	// Assert
	assert.Equal(t, &Plan{
		actions: []model.RenameAction{
			{
				OldPath:     "my-main-branch",
				RenameFrom:  "my-main-branch",
				NewPath:     "main",
				Description: "my-main-branch -> main",
			},
			{
				OldPath:     "my-main-branch/extractor/keboola.ex-db-mysql/my-table",
				RenameFrom:  "main/extractor/keboola.ex-db-mysql/my-table",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables",
				Description: "main/extractor/keboola.ex-db-mysql/{my-table -> 789-tables}",
			},
			{
				OldPath:     "my-main-branch/extractor/keboola.ex-db-mysql/my-table/rows/my-row",
				RenameFrom:  "main/extractor/keboola.ex-db-mysql/789-tables/rows/my-row",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables/rows/12-users",
				Description: "main/extractor/keboola.ex-db-mysql/789-tables/rows/{my-row -> 12-users}",
			},
		},
	}, plan)
}

func loadTestManifest(t *testing.T, inputDir string) (*manifest.Manifest, filesystem.Fs) {
	t.Helper()

	// Create Fs
	fs := testhelper.NewMemoryFsFrom(inputDir)

	// Replace ENVs
	envs := env.Empty()
	envs.Set("LOCAL_PROJECT_ID", "12345")
	envs.Set("TEST_KBC_STORAGE_API_HOST", "foo.bar")
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Load manifest
	m, err := manifest.Load(fs)
	assert.NoError(t, err)

	return m, fs
}
