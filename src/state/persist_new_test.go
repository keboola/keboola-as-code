package state

import (
	"context"
	"github.com/jarcoal/httpmock"
	"github.com/otiai10/copy"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/utils"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestPersistNoChange(t *testing.T) {
	projectDir := initMinimalProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic Extractor",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)

	// State before
	logger, _ := utils.NewDebugLogger()
	state := newState(NewOptions(m, api, context.Background(), logger))
	assert.NotNil(t, state)
	state.doLoadLocalState()
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Empty(t, state.UntrackedPaths())

	// State after
	persisted, err := state.PersistNew()
	assert.NoError(t, err)
	assert.Empty(t, persisted)
}

func TestPersistNewConfig(t *testing.T) {
	projectDir := initMinimalProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
	api, _ := remote.TestMockedStorageApi(t)

	// Mocked API response
	getGenericExResponder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "ex-generic-v2",
		"type": "extractor",
		"name": "Generic Extractor",
	})
	assert.NoError(t, err)
	generateNewIdResponser, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id": "12345",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	httpmock.RegisterResponder("POST", `=~/storage/tickets`, generateNewIdResponser.Once())

	// Write files
	configDir := filepath.Join(projectDir, `main`, `extractor`, `ex-generic-v2`, `new-config`)
	assert.NoError(t, os.Mkdir(configDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `config.json`), []byte(`{"key": "value"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `meta.json`), []byte(`{"name": "foo", "description": "bar"}`), 0644))

	// State before
	logger, _ := utils.NewDebugLogger()
	state := newState(NewOptions(m, api, context.Background(), logger))
	assert.NotNil(t, state)
	state.doLoadLocalState()
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2/new-config",
		"main/extractor/ex-generic-v2/new-config/config.json",
		"main/extractor/ex-generic-v2/new-config/meta.json",
	}, state.UntrackedPaths())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Len(t, state.All(), 2)

	// State after
	persisted, err := state.PersistNew()
	persistedPaths := make([]string, 0)
	for _, object := range persisted {
		persistedPaths = append(persistedPaths, object.RelativePath())
	}
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2/new-config",
	}, persistedPaths)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 2)
	assert.Len(t, state.All(), 3)
	configKey := model.ConfigKey{BranchId: 111, ComponentId: "ex-generic-v2", Id: "12345"}
	assert.Equal(
		t,
		&ConfigState{
			ConfigManifest: &manifest.ConfigManifest{
				ConfigKey: configKey,
				RecordState: manifest.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: manifest.Paths{
					ParentPath: "main",
					Path:       "extractor/ex-generic-v2/new-config",
				},
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: configKey.ComponentId,
				},
				Type: "extractor",
				Name: "Generic Extractor",
			},
			Remote: nil,
			Local: &model.Config{
				ConfigKey:   configKey,
				Name:        "foo",
				Description: "bar",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key:   "key",
						Value: "value",
					},
				}),
			},
		},
		state.GetConfig(configKey, false),
	)
}

func TestPersistNewConfigRow(t *testing.T) {
	projectDir := initMinimalProjectDir(t)
	metadataDir := filepath.Join(projectDir, manifest.MetadataDir)
	m, err := manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)
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
	generateNewIdResponse1, err := httpmock.NewJsonResponse(200, map[string]interface{}{"id": "12345"})
	assert.NoError(t, err)
	generateNewIdResponse2, err := httpmock.NewJsonResponse(200, map[string]interface{}{"id": "45678"})
	assert.NoError(t, err)
	generateNewIdResponder := httpmock.ResponderFromMultipleResponses([]*http.Response{generateNewIdResponse1, generateNewIdResponse2})
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder.Once())
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getMySqlExResponder.Once())
	httpmock.RegisterResponder("POST", `=~/storage/tickets`, generateNewIdResponder.Times(2))

	// Write files
	configDir := filepath.Join(projectDir, `main`, `extractor`, `keboola.ex-db-mysql`, `new-config`)
	assert.NoError(t, os.MkdirAll(configDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `config.json`), []byte(`{"key1": "value1"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `meta.json`), []byte(`{"name": "foo1", "description": "bar1"}`), 0644))
	rowDir := filepath.Join(configDir, `rows`, `some-row`)
	assert.NoError(t, os.MkdirAll(rowDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(rowDir, `config.json`), []byte(`{"key2": "value2"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(rowDir, `meta.json`), []byte(`{"name": "foo2", "description": "bar2"}`), 0644))

	// State before
	logger, _ := utils.NewDebugLogger()
	options := NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	state, ok := LoadState(options)
	assert.True(t, ok)
	assert.Empty(t, state.LocalErrors().Errors)
	assert.Equal(t, []string{
		"main/extractor/keboola.ex-db-mysql",
		"main/extractor/keboola.ex-db-mysql/new-config",
		"main/extractor/keboola.ex-db-mysql/new-config/config.json",
		"main/extractor/keboola.ex-db-mysql/new-config/meta.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/config.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/meta.json",
	}, state.UntrackedPaths())
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 1)
	assert.Len(t, state.ConfigRows(), 0)
	assert.Len(t, state.All(), 2)

	// State after
	persisted, err := state.PersistNew()
	persistedPaths := make([]string, 0)
	for _, object := range persisted {
		persistedPaths = append(persistedPaths, object.RelativePath())
	}
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"main/extractor/keboola.ex-db-mysql/new-config",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row",
	}, persistedPaths)
	assert.Len(t, state.Branches(), 1)
	assert.Len(t, state.Configs(), 2)
	assert.Len(t, state.ConfigRows(), 1)
	assert.Len(t, state.All(), 4)
	rowKey := model.ConfigRowKey{BranchId: 111, ComponentId: "keboola.ex-db-mysql", ConfigId: "12345", Id: "45678"}
	configKey := rowKey.ConfigKey()
	assert.Equal(
		t,
		&ConfigState{
			ConfigManifest: &manifest.ConfigManifest{
				ConfigKey: *configKey,
				RecordState: manifest.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: manifest.Paths{
					ParentPath: "main",
					Path:       "extractor/keboola.ex-db-mysql/new-config",
				},
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: configKey.ComponentId,
				},
				Type: "extractor",
				Name: "MySQL Extractor",
			},
			Remote: nil,
			Local: &model.Config{
				ConfigKey:   *configKey,
				Name:        "foo1",
				Description: "bar1",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key:   "key1",
						Value: "value1",
					},
				}),
			},
		},
		state.GetConfig(*configKey, false),
	)
	assert.Equal(
		t,
		&ConfigRowState{
			ConfigRowManifest: &manifest.ConfigRowManifest{
				ConfigRowKey: rowKey,
				RecordState: manifest.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: manifest.Paths{
					ParentPath: "main/extractor/keboola.ex-db-mysql/new-config",
					Path:       "rows/some-row",
				},
			},
			Remote: nil,
			Local: &model.ConfigRow{
				ConfigRowKey: rowKey,
				Name:         "foo2",
				Description:  "bar2",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{
						Key:   "key2",
						Value: "value2",
					},
				}),
			},
		},
		state.GetConfigRow(rowKey, false),
	)
}

func initMinimalProjectDir(t *testing.T) string {
	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", "456")

	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := t.TempDir()
	err := copy.Copy(filepath.Join(testDir, `..`, `fixtures`, `local`, `minimal`), projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir)

	return projectDir
}
