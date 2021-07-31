package state

import (
	"context"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"path/filepath"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
)

func TestPersistDeleted(t *testing.T) {
	projectDir := initMinimalProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic Extractor",
	})
	assert.NoError(t, err)
	getMySqlExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "keboola.ex-db-mysql",
		"type": "extractor",
		"name": "MySQL Extractor",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)
	httpmock.RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySqlExResponder)

	// Update manifest, add fake records
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	branchId := cast.ToInt(utils.MustGetEnv(`LOCAL_STATE_MAIN_BRANCH_ID`))
	m.PersistRecord(&model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    branchId,
			ComponentId: "keboola.ex-db-mysql",
			Id:          "101",
		},
		Paths: model.Paths{
			ParentPath: "main",
			Path:       "extractor/keboola.ex-db-mysql/missing",
		},
	})
	m.PersistRecord(&model.ConfigRowManifest{
		ConfigRowKey: model.ConfigRowKey{
			BranchId:    branchId,
			ComponentId: "keboola.ex-db-mysql",
			ConfigId:    "101",
			Id:          "202",
		},
		Paths: model.Paths{
			ParentPath: "main/extractor/keboola.ex-db-mysql/missing",
			Path:       "rows/some-row",
		},
	})
	assert.NoError(t, m.Save())

	// Reload manifest
	m, err = manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)

	// State before
	logger, _ := utils.NewDebugLogger()
	options := NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	options.SkipNotFoundErr = true
	state, ok := LoadState(options)
	assert.True(t, ok)
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Empty(t, state.UntrackedPaths())

	// State after
	deleted, err := state.PersistDeleted()
	deletedPaths := make([]string, 0)
	for _, record := range deleted {
		deletedPaths = append(deletedPaths, record.RelativePath())
	}
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"main/extractor/keboola.ex-db-mysql/missing",
		"main/extractor/keboola.ex-db-mysql/missing/rows/some-row",
	}, deletedPaths)
}
