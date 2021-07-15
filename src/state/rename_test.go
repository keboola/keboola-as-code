package state

import (
	"context"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/local"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"path/filepath"
	"runtime"
	"testing"
)

func TestRenameAllPlan(t *testing.T) {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := filepath.Join(testDir, "..", "fixtures", "local", "to-rename")
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	if err != nil {
		assert.FailNow(t, err.Error())
	}

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
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	httpmock.RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySqlExResponder.Once())

	// Load state
	logger, _ := utils.NewDebugLogger()
	api, _ := remote.TestMockedStorageApi(t)
	options := NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	state, ok := LoadState(options)
	if !ok {
		assert.FailNow(t, state.LocalErrors().Error())
	}

	// Get rename plan
	assert.Equal(t, []*local.RenamePlan{
		{
			OldPath:     filepath.Join(projectDir, "my-main-branch"),
			NewPath:     filepath.Join(projectDir, "main"),
			Description: "my-main-branch -> main",
		},
		{
			OldPath:     filepath.Join(projectDir, "main/extractor/keboola.ex-db-mysql/my-table"),
			NewPath:     filepath.Join(projectDir, "main/extractor/keboola.ex-db-mysql/789-tables"),
			Description: "main/extractor/keboola.ex-db-mysql/{my-table -> 789-tables}",
		},
		{
			OldPath:     filepath.Join(projectDir, "main/extractor/keboola.ex-db-mysql/789-tables/rows/my-row"),
			NewPath:     filepath.Join(projectDir, "main/extractor/keboola.ex-db-mysql/789-tables/rows/12-users"),
			Description: "main/extractor/keboola.ex-db-mysql/789-tables/rows/{my-row -> 12-users}",
		},
	}, state.RenamePlan())
}
