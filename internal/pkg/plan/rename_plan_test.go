package plan

import (
	"context"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestRenameAllPlan(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	m, _ := loadTestManifest(t, filesystem.Join(testDir, "..", "fixtures", "local", "to-rename"))

	// Load state
	logger, _ := utils.NewDebugLogger()
	api, httpTransport, _ := remote.TestMockedStorageApi(t)

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
	options := state.NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	projectState, ok := state.LoadState(options)
	if !ok {
		assert.FailNow(t, projectState.LocalErrors().Error())
	}

	// Get rename plan
	plan, err := Rename(projectState)
	assert.NoError(t, err)

	// Clear manifest records before assert
	for _, action := range plan.actions {
		action.Record = nil
	}

	// Assert
	assert.Equal(t, &RenamePlan{
		actions: []*RenameAction{
			{
				OldPath:     "my-main-branch",
				NewPath:     "main",
				Description: "my-main-branch -> main",
			},
			{
				OldPath:     "main/extractor/keboola.ex-db-mysql/my-table",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables",
				Description: "main/extractor/keboola.ex-db-mysql/{my-table -> 789-tables}",
			},
			{
				OldPath:     "main/extractor/keboola.ex-db-mysql/789-tables/rows/my-row",
				NewPath:     "main/extractor/keboola.ex-db-mysql/789-tables/rows/12-users",
				Description: "main/extractor/keboola.ex-db-mysql/789-tables/rows/{my-row -> 12-users}",
			},
		},
	}, plan)
}
