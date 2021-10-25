package plan

import (
	"context"
	"net/http"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type testCase struct {
	inputDir        string
	untrackedPaths  []string
	expectedNewIds  int
	expectedPlan    []PersistAction
	expectedStates  []model.ObjectState
	expectedMissing []model.Key
}

func TestPersistNoChange(t *testing.T) {
	t.Parallel()
	tc := testCase{
		inputDir:       `persist-no-change`,
		untrackedPaths: nil,
		expectedPlan:   nil,
	}
	tc.run(t)
}

func TestPersistNewConfig(t *testing.T) {
	t.Parallel()
	tc := testCase{
		inputDir: `persist-config`,
		untrackedPaths: []string{
			"main/extractor/ex-generic-v2/new-config",
			"main/extractor/ex-generic-v2/new-config/config.json",
			"main/extractor/ex-generic-v2/new-config/description.md",
			"main/extractor/ex-generic-v2/new-config/meta.json",
		},
		expectedNewIds: 1,
		expectedPlan: []PersistAction{
			&NewConfigAction{
				PathInProject: model.NewPathInProject(
					"main",
					"extractor/ex-generic-v2/new-config",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: "ex-generic-v2",
				},
			},
		},
		expectedStates: []model.ObjectState{
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: "ex-generic-v2",
						Id:          "1001",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main",
							"extractor/ex-generic-v2/new-config",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: "ex-generic-v2",
						Id:          "1001",
					},
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
		},
	}
	tc.run(t)
}

func TestPersistNewConfigRow(t *testing.T) {
	t.Parallel()
	tc := testCase{
		inputDir: `persist-config-row`,
		untrackedPaths: []string{
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
		},
		expectedNewIds: 2,
		expectedPlan: []PersistAction{
			&NewConfigAction{
				PathInProject: model.NewPathInProject(
					"main",
					"extractor/keboola.ex-db-mysql/new-config",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: "keboola.ex-db-mysql",
				},
			},
			&NewRowAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/keboola.ex-db-mysql/new-config",
					"rows/some-row",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: "keboola.ex-db-mysql",
				},
			},
		},
		expectedStates: []model.ObjectState{
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						Id:          "1001",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main",
							"extractor/keboola.ex-db-mysql/new-config",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						Id:          "1001",
					},
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
			&model.ConfigRowState{
				ConfigRowManifest: &model.ConfigRowManifest{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    "1001",
						Id:          "1002",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/extractor/keboola.ex-db-mysql/new-config",
							"rows/some-row",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
				},
				Remote: nil,
				Local: &model.ConfigRow{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    "1001",
						Id:          "1002",
					},
					Name:        "foo2",
					Description: "bar2",
					Content: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key:   "key2",
							Value: "value2",
						},
					}),
				},
			},
		},
	}
	tc.run(t)
}

func TestPersistDeleted(t *testing.T) {
	t.Parallel()
	tc := testCase{
		inputDir:       `persist-deleted`,
		untrackedPaths: nil,
		expectedPlan: []PersistAction{
			&DeleteRecordAction{
				Record: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						Id:          "101",
					},
					RecordState: model.RecordState{
						Invalid:   true,
						NotFound:  true,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main",
							"extractor/keboola.ex-db-mysql/missing",
						),
					},
				},
			},
			&DeleteRecordAction{
				Record: &model.ConfigRowManifest{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: "keboola.ex-db-mysql",
						ConfigId:    "101",
						Id:          "202",
					},
					RecordState: model.RecordState{
						Invalid:   true,
						NotFound:  true,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/extractor/keboola.ex-db-mysql/missing",
							"rows/some-row",
						),
					},
				},
			},
		},
		expectedMissing: []model.Key{
			model.ConfigKey{
				BranchId:    111,
				ComponentId: "keboola.ex-db-mysql",
				Id:          "101",
			},
			model.ConfigRowKey{
				BranchId:    111,
				ComponentId: "keboola.ex-db-mysql",
				ConfigId:    "101",
				Id:          "202",
			},
		},
	}
	tc.run(t)
}

func TestPersistSharedCode(t *testing.T) {
	t.Parallel()
	tc := testCase{
		inputDir: `persist-shared-code`,
		untrackedPaths: []string{
			"main/_shared",
			"main/_shared/keboola.python-transformation-v2",
			"main/_shared/keboola.python-transformation-v2/codes",
			"main/_shared/keboola.python-transformation-v2/codes/my-code",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/code.py",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/config.json",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/description.md",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/meta.json",
			"main/_shared/keboola.python-transformation-v2/config.json",
			"main/_shared/keboola.python-transformation-v2/description.md",
			"main/_shared/keboola.python-transformation-v2/meta.json",
		},
		expectedNewIds: 2,
		expectedPlan: []PersistAction{
			&NewConfigAction{
				PathInProject: model.NewPathInProject(
					"main",
					"_shared/keboola.python-transformation-v2",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
			},
			&NewRowAction{
				PathInProject: model.NewPathInProject(
					"main/_shared/keboola.python-transformation-v2",
					"codes/my-code",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
			},
		},
		expectedStates: []model.ObjectState{
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.SharedCodeComponentId,
						Id:          "1001",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main",
							"_shared/keboola.python-transformation-v2",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.SharedCodeComponentId,
						Id:          "1001",
					},
					Name:        "Shared Codes",
					Description: "foo bar",
					Content: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key:   "componentId",
							Value: "keboola.python-transformation-v2",
						},
					}),
				},
			},
			&model.ConfigRowState{
				ConfigRowManifest: &model.ConfigRowManifest{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: model.SharedCodeComponentId,
						ConfigId:    "1001",
						Id:          "1002",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/_shared/keboola.python-transformation-v2",
							"codes/my-code",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile, `code.py`},
					},
				},
				Remote: nil,
				Local: &model.ConfigRow{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: model.SharedCodeComponentId,
						ConfigId:    "1001",
						Id:          "1002",
					},
					Name:        "My code",
					Description: "test code",
					Content: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key:   "code_content",
							Value: "print('Hello, world!')\n",
						},
					}),
				},
			},
		},
	}
	tc.run(t)
}

func TestPersistVariables(t *testing.T) {
	t.Parallel()

	expectedRelations := model.Relations{
		&model.VariablesForRelation{
			Target: model.ConfigKeySameBranch{
				ComponentId: `ex-generic-v2`,
				Id:          `456`,
			},
		},
	}

	tc := testCase{
		inputDir: `persist-variables`,
		untrackedPaths: []string{
			"main/extractor/ex-generic-v2/456-todos/variables",
			"main/extractor/ex-generic-v2/456-todos/variables/config.json",
			"main/extractor/ex-generic-v2/456-todos/variables/description.md",
			"main/extractor/ex-generic-v2/456-todos/variables/meta.json",
			"main/extractor/ex-generic-v2/456-todos/variables/values",
			"main/extractor/ex-generic-v2/456-todos/variables/values/default",
			"main/extractor/ex-generic-v2/456-todos/variables/values/default/config.json",
			"main/extractor/ex-generic-v2/456-todos/variables/values/default/description.md",
			"main/extractor/ex-generic-v2/456-todos/variables/values/default/meta.json",
		},
		expectedNewIds: 2,
		expectedPlan: []PersistAction{
			&NewConfigAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/ex-generic-v2/456-todos",
					"variables",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.VariablesComponentId,
				},
				ParentConfig: &model.ConfigKeySameBranch{
					ComponentId: `ex-generic-v2`,
					Id:          `456`,
				},
			},
			&NewRowAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/ex-generic-v2/456-todos/variables",
					"values/default",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: model.VariablesComponentId,
				},
			},
		},
		expectedStates: []model.ObjectState{
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						Id:          "1001",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/extractor/ex-generic-v2/456-todos",
							"variables",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
					Relations: expectedRelations,
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						Id:          "1001",
					},
					Name:        "Variables",
					Description: "test1",
					Content: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key: "variables",
							Value: []interface{}{
								*utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "name",
										Value: "foo",
									},
									{
										Key:   "type",
										Value: "string",
									},
								}),
							},
						},
					}),
					Relations: expectedRelations,
				},
			},
			&model.ConfigRowState{
				ConfigRowManifest: &model.ConfigRowManifest{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						ConfigId:    "1001",
						Id:          "1002",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/extractor/ex-generic-v2/456-todos/variables",
							"values/default",
						),
						RelatedPaths: []string{model.MetaFile, model.ConfigFile, model.DescriptionFile},
					},
				},
				Remote: nil,
				Local: &model.ConfigRow{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						ConfigId:    "1001",
						Id:          "1002",
					},
					Name:        "Default values",
					Description: "test2",
					Content: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key: "values",
							Value: []interface{}{
								*utils.PairsToOrderedMap([]utils.Pair{
									{
										Key:   "name",
										Value: "foo",
									},
									{
										Key:   "value",
										Value: "bar",
									},
								}),
							},
						},
					}),
				},
			},
		},
	}
	tc.run(t)
}

func (tc *testCase) run(t *testing.T) {
	t.Helper()

	// Init project dir
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	inputDir := filesystem.Join(testDir, `..`, `fixtures`, `local`, tc.inputDir)

	// Create Fs
	fs := testhelper.NewMemoryFsFrom(inputDir)

	// Load manifest
	m, err := manifest.LoadManifest(fs)
	assert.NoError(t, err)

	// Create API
	api, httpTransport, _ := testapi.TestMockedStorageApi()
	testapi.AddMockedComponents(httpTransport)

	// Register new IDs API responses
	var ticketResponses []*http.Response
	for i := 1; i <= tc.expectedNewIds; i++ {
		response, err := httpmock.NewJsonResponse(200, map[string]interface{}{"id": cast.ToString(1000 + i)})
		assert.NoError(t, err)
		ticketResponses = append(ticketResponses, response)
	}
	httpTransport.RegisterResponder("POST", `=~/storage/tickets`, httpmock.ResponderFromMultipleResponses(ticketResponses))

	// Load state
	logger, _ := utils.NewDebugLogger()
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	options := state.NewOptions(m, api, schedulerApi, context.Background(), logger)

	options.LoadLocalState = true
	options.LoadRemoteState = false
	options.SkipNotFoundErr = true
	projectState, ok := state.LoadState(options)
	assert.NotNil(t, projectState)
	assert.True(t, ok)
	assert.NoError(t, projectState.LocalErrors().ErrorOrNil())

	// Assert state before
	assert.Equal(t, tc.untrackedPaths, projectState.UntrackedPaths())
	for _, objectState := range tc.expectedStates {
		_, found := projectState.Get(objectState.Key())
		assert.Falsef(t, found, `%s should not exists`, objectState.Desc())
	}
	for _, key := range tc.expectedMissing {
		_, found := m.GetRecord(key)
		assert.Truef(t, found, `%s should exists`, key.Desc())
	}

	// Get plan
	plan, err := Persist(projectState)
	assert.NoError(t, err)

	// Delete callbacks for easier comparison (we only check callbacks result)
	for _, action := range plan.actions {
		if a, ok := action.(*NewConfigAction); ok {
			a.OnPersist = nil
		}
	}

	// Assert plan
	assert.Equalf(t, tc.expectedPlan, plan.actions, `unexpected persist plan`)

	// Invoke
	plan, err = Persist(projectState) // plan with callbacks
	assert.NoError(t, err)
	assert.NoError(t, plan.Invoke(logger, api, projectState))

	// Assert new IDs requests count
	assert.Equal(t, tc.expectedNewIds, httpTransport.GetCallCountInfo()["POST =~/storage/tickets"])

	// Assert state after
	assert.Empty(t, projectState.UntrackedPaths())
	for _, objectState := range tc.expectedStates {
		realState, found := projectState.Get(objectState.Key())
		assert.Truef(t, found, `%s should exists`, objectState.Desc())
		assert.Equalf(t, objectState, realState, `object "%s" has unexpected content`, objectState.Desc())
	}
	for _, key := range tc.expectedMissing {
		_, found := m.GetRecord(key)
		assert.Falsef(t, found, `%s should not exists`, key.Desc())
	}
}
