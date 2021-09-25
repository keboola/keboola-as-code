package plan

import (
	"context"
	"net/http"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/nhatthm/aferocopy"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/thelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestPersistNoChange(t *testing.T) {
	m, _ := loadTestManifest(t, initMinimalProjectDir(t))
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
	m, fs := loadTestManifest(t, initMinimalProjectDir(t))
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
	configDir := filesystem.Join(`main`, `extractor`, `ex-generic-v2`, `new-config`)
	assert.NoError(t, fs.Mkdir(configDir))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `config.json`), `{"key": "value"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `meta.json`), `{"name": "foo"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `description.md`), `bar`)))

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
		"main/extractor/ex-generic-v2/new-config/description.md",
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
					PathInProject: model.PathInProject{
						ParentPath: "main",
						ObjectPath: "extractor/ex-generic-v2/new-config",
					},
					RelatedPaths: []string{model.MetaFile, model.DescriptionFile, model.ConfigFile},
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
	m, fs := loadTestManifest(t, initMinimalProjectDir(t))
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
	configDir := filesystem.Join(`main`, `extractor`, `keboola.ex-db-mysql`, `new-config`)
	assert.NoError(t, fs.Mkdir(configDir))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `config.json`), `{"key1": "value1"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `meta.json`), `{"name": "foo1"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(configDir, `description.md`), `bar1`)))
	rowDir := filesystem.Join(configDir, `rows`, `some-row`)
	assert.NoError(t, fs.Mkdir(rowDir))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(rowDir, `config.json`), `{"key2": "value2"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(rowDir, `meta.json`), `{"name": "foo2"}`)))
	assert.NoError(t, fs.WriteFile(filesystem.CreateFile(filesystem.Join(rowDir, `description.md`), `bar2`)))

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
		"main/extractor/keboola.ex-db-mysql/new-config/description.md",
		"main/extractor/keboola.ex-db-mysql/new-config/meta.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/config.json",
		"main/extractor/keboola.ex-db-mysql/new-config/rows/some-row/description.md",
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
					PathInProject: model.PathInProject{
						ParentPath: "main",
						ObjectPath: "extractor/keboola.ex-db-mysql/new-config",
					},
					RelatedPaths: []string{model.MetaFile, model.DescriptionFile, model.ConfigFile},
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
					PathInProject: model.PathInProject{
						ParentPath: "main/extractor/keboola.ex-db-mysql/new-config",
						ObjectPath: "rows/some-row",
					},
					RelatedPaths: []string{model.MetaFile, model.DescriptionFile, model.ConfigFile},
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
	m, _ := loadTestManifest(t, projectDir)
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
	branchId := cast.ToInt(utils.MustGetEnv(`LOCAL_STATE_MAIN_BRANCH_ID`))
	missingConfig := &model.ConfigManifest{
		ConfigKey: model.ConfigKey{
			BranchId:    branchId,
			ComponentId: "keboola.ex-db-mysql",
			Id:          "101",
		},
		Paths: model.Paths{
			PathInProject: model.PathInProject{
				ParentPath: "main",
				ObjectPath: "extractor/keboola.ex-db-mysql/missing",
			},
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
			PathInProject: model.PathInProject{
				ParentPath: "main/extractor/keboola.ex-db-mysql/missing",
				ObjectPath: "rows/some-row",
			},
		},
	}
	assert.NoError(t, m.PersistRecord(missingConfig))
	assert.NoError(t, m.PersistRecord(missingRow))
	assert.NoError(t, m.Save())

	// Reload manifest
	m, _ = loadTestManifest(t, projectDir)

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
	testDir := filesystem.Dir(testFile)
	projectDir := t.TempDir()
	err := aferocopy.Copy(filesystem.Join(testDir, `..`, `fixtures`, `local`, `minimal`), projectDir)
	if err != nil {
		t.Fatalf("Copy error: %s", err)
	}
	thelper.ReplaceEnvsDir(projectDir, nil)

	return projectDir
}
