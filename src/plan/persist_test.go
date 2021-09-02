package plan

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/otiai10/copy"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
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
		"name": "Generic",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)

	// Load state
	logger, _ := utils.NewDebugLogger()
	options := state.NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	options.LoadRemoteState = false
	projectState, ok := state.LoadState(options)
	assert.NotNil(t, projectState)
	assert.True(t, ok)

	// State before
	assert.Empty(t, projectState.UntrackedPaths())

	// Assert plan
	plan := Persist(projectState)
	assert.True(t, plan.Empty())
	assert.Empty(t, plan.actions)

	// Invoke
	assert.NoError(t, plan.Invoke(logger, api, projectState))

	// State after
	assert.Empty(t, projectState.UntrackedPaths())
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
		"name": "Generic",
	})
	assert.NoError(t, err)
	generateNewIdResponser, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id": "12345",
	})
	assert.NoError(t, err)
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)
	httpmock.RegisterResponder("POST", `=~/storage/tickets`, generateNewIdResponser)

	// Write files
	configDir := filepath.Join(projectDir, `main`, `extractor`, `ex-generic-v2`, `new-config`)
	assert.NoError(t, os.Mkdir(configDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `config.json`), []byte(`{"key": "value"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `meta.json`), []byte(`{"name": "foo", "description": "bar"}`), 0644))

	// Load state
	logger, _ := utils.NewDebugLogger()
	options := state.NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	options.LoadRemoteState = false
	projectState, ok := state.LoadState(options)
	assert.NotNil(t, projectState)
	assert.True(t, ok)

	// State before
	assert.Equal(t, []string{
		"main/extractor/ex-generic-v2/new-config",
		"main/extractor/ex-generic-v2/new-config/config.json",
		"main/extractor/ex-generic-v2/new-config/meta.json",
	}, projectState.UntrackedPaths())
	assert.Len(t, projectState.Branches(), 1)
	assert.Len(t, projectState.Configs(), 1)
	assert.Len(t, projectState.All(), 2)

	// Assert plan
	plan := Persist(projectState)
	assert.False(t, plan.Empty())
	assert.Len(t, plan.actions, 1)
	assert.Equal(t, &NewConfigAction{
		Key: model.ConfigKey{
			BranchId:    cast.ToInt(utils.MustGetEnv(`LOCAL_STATE_MAIN_BRANCH_ID`)),
			ComponentId: "ex-generic-v2",
		},
		Path:        "extractor/ex-generic-v2/new-config",
		ProjectPath: "main/extractor/ex-generic-v2/new-config",
		Rows:        nil,
	}, plan.actions[0].(*NewConfigAction))

	// Invoke
	assert.NoError(t, plan.Invoke(logger, api, projectState))

	// State after
	assert.Len(t, projectState.Branches(), 1)
	assert.Len(t, projectState.Configs(), 2)
	assert.Len(t, projectState.All(), 3)
	configKey := model.ConfigKey{BranchId: 111, ComponentId: "ex-generic-v2", Id: "12345"}
	assert.Equal(
		t,
		&model.ConfigState{
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: configKey,
				RecordState: model.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: model.Paths{
					ParentPath:   "main",
					Path:         "extractor/ex-generic-v2/new-config",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
				},
			},
			Component: &model.Component{
				ComponentKey: model.ComponentKey{
					Id: configKey.ComponentId,
				},
				Type: "extractor",
				Name: "Generic",
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
		projectState.Get(configKey),
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
		"name": "Generic",
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
	httpmock.RegisterResponder("GET", `=~/storage/components/ex-generic-v2`, getGenericExResponder)
	httpmock.RegisterResponder("GET", `=~/storage/components/keboola.ex-db-mysql`, getMySqlExResponder)
	httpmock.RegisterResponder("POST", `=~/storage/tickets`, generateNewIdResponder)

	// Write files
	configDir := filepath.Join(projectDir, `main`, `extractor`, `keboola.ex-db-mysql`, `new-config`)
	assert.NoError(t, os.MkdirAll(configDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `config.json`), []byte(`{"key1": "value1"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(configDir, `meta.json`), []byte(`{"name": "foo1", "description": "bar1"}`), 0644))
	rowDir := filepath.Join(configDir, `rows`, `some-row`)
	assert.NoError(t, os.MkdirAll(rowDir, 0755))
	assert.NoError(t, os.WriteFile(filepath.Join(rowDir, `config.json`), []byte(`{"key2": "value2"}`), 0644))
	assert.NoError(t, os.WriteFile(filepath.Join(rowDir, `meta.json`), []byte(`{"name": "foo2", "description": "bar2"}`), 0644))

	// Load state
	logger, _ := utils.NewDebugLogger()
	options := state.NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	options.LoadRemoteState = false
	projectState, ok := state.LoadState(options)
	assert.NotNil(t, projectState)
	assert.True(t, ok)

	// State before
	assert.Equal(t, []string{
		"main/extractor/keboola.ex-db-mysql",
		"main/extractor/keboola.ex-db-mysql/new-config",
		"main/extractor/keboola.ex-db-mysql/new-config/config.json",
		"main/extractor/keboola.ex-db-mysql/new-config/meta.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/config.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/meta.json",
	}, projectState.UntrackedPaths())
	assert.Len(t, projectState.Branches(), 1)
	assert.Len(t, projectState.Configs(), 1)
	assert.Len(t, projectState.ConfigRows(), 0)
	assert.Len(t, projectState.All(), 2)

	// Assert plan
	plan := Persist(projectState)
	assert.False(t, plan.Empty())
	assert.Len(t, plan.actions, 2)
	rowAction := &NewRowAction{
		Key: model.ConfigRowKey{
			BranchId:    cast.ToInt(utils.MustGetEnv(`LOCAL_STATE_MAIN_BRANCH_ID`)),
			ComponentId: "keboola.ex-db-mysql",
		},
		Path:        "rows/some-row",
		ProjectPath: "main/extractor/keboola.ex-db-mysql/new-config/rows/some-row",
	}
	configAction := &NewConfigAction{
		Key: model.ConfigKey{
			BranchId:    cast.ToInt(utils.MustGetEnv(`LOCAL_STATE_MAIN_BRANCH_ID`)),
			ComponentId: "keboola.ex-db-mysql",
		},
		Path:        "extractor/keboola.ex-db-mysql/new-config",
		ProjectPath: "main/extractor/keboola.ex-db-mysql/new-config",
		Rows:        []*NewRowAction{rowAction},
	}
	assert.Equal(t, []PersistAction{configAction, rowAction}, plan.actions)

	// Invoke
	assert.NoError(t, plan.Invoke(logger, api, projectState))

	// State after
	assert.Len(t, projectState.Branches(), 1)
	assert.Len(t, projectState.Configs(), 2)
	assert.Len(t, projectState.ConfigRows(), 1)
	assert.Len(t, projectState.All(), 4)
	rowKey := model.ConfigRowKey{BranchId: 111, ComponentId: "keboola.ex-db-mysql", ConfigId: "12345", Id: "45678"}
	configKey := rowKey.ConfigKey()
	assert.Equal(
		t,
		&model.ConfigState{
			ConfigManifest: &model.ConfigManifest{
				ConfigKey: *configKey,
				RecordState: model.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: model.Paths{
					ParentPath:   "main",
					Path:         "extractor/keboola.ex-db-mysql/new-config",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
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
		projectState.Get(*configKey),
	)
	assert.Equal(
		t,
		&model.ConfigRowState{
			ConfigRowManifest: &model.ConfigRowManifest{
				ConfigRowKey: rowKey,
				RecordState: model.RecordState{
					Invalid:   false,
					Persisted: true,
				},
				Paths: model.Paths{
					ParentPath:   "main/extractor/keboola.ex-db-mysql/new-config",
					Path:         "rows/some-row",
					RelatedPaths: []string{model.MetaFile, model.ConfigFile},
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
		projectState.Get(rowKey),
	)
}

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
	missingConfig := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    branchId,
			ComponentId: "keboola.ex-db-mysql",
			Id:          "101",
		},
		Paths: model.Paths{
			ParentPath: "main",
			Path:       "extractor/keboola.ex-db-mysql/missing",
		},
	}
	missingRow := &model.ConfigRowManifest{
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
	}
	m.PersistRecord(missingConfig)
	m.PersistRecord(missingRow)
	assert.NoError(t, m.Save())

	// Reload manifest
	m, err = manifest.LoadManifest(projectDir, metadataDir)
	assert.NoError(t, err)

	// Load state
	logger, _ := utils.NewDebugLogger()
	options := state.NewOptions(m, api, context.Background(), logger)
	options.LoadLocalState = true
	options.LoadRemoteState = false
	options.SkipNotFoundErr = true
	projectState, ok := state.LoadState(options)
	assert.NotNil(t, projectState)
	assert.True(t, ok)

	// State before
	assert.Empty(t, projectState.UntrackedPaths())

	// Assert plan
	plan := Persist(projectState)

	// Invoke
	assert.NoError(t, plan.Invoke(logger, api, projectState))

	// State after
	_, configFound := m.GetRecord(missingConfig.Key())
	assert.False(t, configFound)
	_, rowFound := m.GetRecord(missingRow.Key())
	assert.False(t, rowFound)
}

func initMinimalProjectDir(t *testing.T) string {
	t.Helper()

	utils.MustSetEnv("LOCAL_STATE_MAIN_BRANCH_ID", "111")
	utils.MustSetEnv("LOCAL_STATE_GENERIC_CONFIG_ID", "456")

	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	projectDir := t.TempDir()
	err := copy.Copy(filepath.Join(testDir, `..`, `fixtures`, `local`, `minimal`), projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	utils.ReplaceEnvsDir(projectDir, nil)

	return projectDir
}
