package persist

import (
	"net/http"
	"runtime"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type testCase struct {
	inputDir        string
	untrackedPaths  []string
	expectedNewIds  int
	expectedPlan    []action
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
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main",
					"extractor/ex-generic-v2/new-config",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: "ex-generic-v2",
				},
				ParentKey: model.BranchKey{
					Id: 111,
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
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
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main",
					"extractor/keboola.ex-db-mysql/new-config",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: "keboola.ex-db-mysql",
				},
				ParentKey: model.BranchKey{
					Id: 111,
				},
			},
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/keboola.ex-db-mysql/new-config",
					"rows/some-row",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: "keboola.ex-db-mysql",
				},
				ParentKey: model.ConfigKey{
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
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
		expectedPlan: []action{
			&deleteManifestRecordAction{
				ObjectManifest: &model.ConfigManifest{
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
			&deleteManifestRecordAction{
				ObjectManifest: &model.ConfigRowManifest{
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
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main",
					"_shared/keboola.python-transformation-v2",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
				ParentKey: model.BranchKey{
					Id: 111,
				},
			},
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/_shared/keboola.python-transformation-v2",
					"codes/my-code",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
				ParentKey: model.ConfigKey{
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
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
					Content:     orderedmap.New(),
					SharedCode: &model.SharedCodeConfig{
						Target: model.ComponentId("keboola.python-transformation-v2"),
					},
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile, `code.py`},
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
					Content:     orderedmap.New(),
					SharedCode: &model.SharedCodeRow{
						Target: "keboola.python-transformation-v2",
						Scripts: model.Scripts{
							model.StaticScript{Value: "print('Hello, world!')"},
						},
					},
				},
			},
		},
	}
	tc.run(t)
}

func TestPersistSharedCodeWithVariables(t *testing.T) {
	expectedCodeRelations := model.Relations{
		&model.SharedCodeVariablesFromRelation{
			VariablesId: `1003`,
		},
	}
	expectedVariablesRelations := model.Relations{
		&model.SharedCodeVariablesForRelation{
			ConfigId: `1001`,
			RowId:    `1002`,
		},
	}

	t.Parallel()
	tc := testCase{
		inputDir: `persist-shared-code-with-vars`,
		untrackedPaths: []string{
			"main/_shared",
			"main/_shared/keboola.python-transformation-v2",
			"main/_shared/keboola.python-transformation-v2/codes",
			"main/_shared/keboola.python-transformation-v2/codes/my-code",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/code.py",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/config.json",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/description.md",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/meta.json",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/variables",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/variables/config.json",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/variables/description.md",
			"main/_shared/keboola.python-transformation-v2/codes/my-code/variables/meta.json",
			"main/_shared/keboola.python-transformation-v2/config.json",
			"main/_shared/keboola.python-transformation-v2/description.md",
			"main/_shared/keboola.python-transformation-v2/meta.json",
		},
		expectedNewIds: 3,
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main",
					"_shared/keboola.python-transformation-v2",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
				ParentKey: model.BranchKey{
					Id: 111,
				},
			},
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/_shared/keboola.python-transformation-v2",
					"codes/my-code",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
				ParentKey: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.SharedCodeComponentId,
				},
			},
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/_shared/keboola.python-transformation-v2/codes/my-code",
					"variables",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.VariablesComponentId,
				},
				ParentKey: model.ConfigRowKey{
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
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
					Content:     orderedmap.New(),
					SharedCode: &model.SharedCodeConfig{
						Target: model.ComponentId("keboola.python-transformation-v2"),
					},
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile, `code.py`},
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
					Content:     orderedmap.New(),
					SharedCode: &model.SharedCodeRow{
						Target: "keboola.python-transformation-v2",
						Scripts: model.Scripts{
							model.StaticScript{
								Value: "num1 = {{num1}}\nnum2 = {{num2}}\nsum = num1 + num2",
							},
						},
					},
					Relations: expectedCodeRelations,
				},
			},
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						Id:          "1003",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/_shared/keboola.python-transformation-v2/codes/my-code",
							"variables",
						),
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
					},
					Relations: expectedVariablesRelations,
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.VariablesComponentId,
						Id:          "1003",
					},
					Name:        "Shared Code Variables",
					Description: "test fixture",
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "variables",
							Value: []interface{}{
								orderedmap.FromPairs([]orderedmap.Pair{
									{
										Key:   "name",
										Value: "num1",
									},
									{
										Key:   "type",
										Value: "string",
									},
								}),
								orderedmap.FromPairs([]orderedmap.Pair{
									{
										Key:   "name",
										Value: "num2",
									},
									{
										Key:   "type",
										Value: "string",
									},
								}),
							},
						},
					}),
					Relations: expectedVariablesRelations,
				},
			},
		},
	}
	tc.run(t)
}

func TestPersistVariables(t *testing.T) {
	t.Parallel()

	expectedConfigRelations := model.Relations{
		&model.VariablesForRelation{
			ComponentId: `ex-generic-v2`,
			ConfigId:    `456`,
		},
	}
	expectedRowRelations := model.Relations{
		&model.VariablesValuesForRelation{},
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
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/ex-generic-v2/456-todos",
					"variables",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.VariablesComponentId,
				},
				ParentKey: model.ConfigKey{
					BranchId:    111,
					ComponentId: `ex-generic-v2`,
					Id:          `456`,
				},
			},
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/ex-generic-v2/456-todos/variables",
					"values/default",
				),
				Key: model.ConfigRowKey{
					BranchId:    111,
					ComponentId: model.VariablesComponentId,
				},
				ParentKey: model.ConfigKey{
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
					},
					Relations: expectedConfigRelations,
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "variables",
							Value: []interface{}{
								orderedmap.FromPairs([]orderedmap.Pair{
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
					Relations: expectedConfigRelations,
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
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
					},
					Relations: expectedRowRelations,
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "values",
							Value: []interface{}{
								orderedmap.FromPairs([]orderedmap.Pair{
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
					Relations: expectedRowRelations,
				},
			},
		},
	}
	tc.run(t)
}

func TestPersistScheduler(t *testing.T) {
	t.Parallel()

	expectedRelations := model.Relations{
		&model.SchedulerForRelation{
			ComponentId: `ex-generic-v2`,
			ConfigId:    `456`,
		},
	}

	expectedContentStr := `
{
  "schedule": {
    "cronTab": "*/10 * * * *",
    "timezone": "UTC",
    "state": "disabled"
  },
  "target": {
    "mode": "run"
  }
}
`
	expectedContent := orderedmap.New()
	json.MustDecodeString(expectedContentStr, expectedContent)

	tc := testCase{
		inputDir: `persist-scheduler`,
		untrackedPaths: []string{
			"main/extractor/ex-generic-v2/456-todos/schedules",
			"main/extractor/ex-generic-v2/456-todos/schedules/my-scheduler",
			"main/extractor/ex-generic-v2/456-todos/schedules/my-scheduler/config.json",
			"main/extractor/ex-generic-v2/456-todos/schedules/my-scheduler/description.md",
			"main/extractor/ex-generic-v2/456-todos/schedules/my-scheduler/meta.json",
		},
		expectedNewIds: 1,
		expectedPlan: []action{
			&newObjectAction{
				PathInProject: model.NewPathInProject(
					"main/extractor/ex-generic-v2/456-todos",
					"schedules/my-scheduler",
				),
				Key: model.ConfigKey{
					BranchId:    111,
					ComponentId: model.SchedulerComponentId,
				},
				ParentKey: model.ConfigKey{
					BranchId:    111,
					ComponentId: `ex-generic-v2`,
					Id:          `456`,
				},
			},
		},
		expectedStates: []model.ObjectState{
			&model.ConfigState{
				ConfigManifest: &model.ConfigManifest{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.SchedulerComponentId,
						Id:          "1001",
					},
					RecordState: model.RecordState{
						Invalid:   false,
						Persisted: true,
					},
					Paths: model.Paths{
						PathInProject: model.NewPathInProject(
							"main/extractor/ex-generic-v2/456-todos",
							"schedules/my-scheduler",
						),
						RelatedPaths: []string{naming.MetaFile, naming.ConfigFile, naming.DescriptionFile},
					},
					Relations: expectedRelations,
				},
				Remote: nil,
				Local: &model.Config{
					ConfigKey: model.ConfigKey{
						BranchId:    111,
						ComponentId: model.SchedulerComponentId,
						Id:          "1001",
					},
					Name:        "My Scheduler",
					Description: "",
					Content:     expectedContent,
					Relations:   expectedRelations,
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
	inputDir := filesystem.Join(testDir, `..`, `..`, `fixtures`, `local`, tc.inputDir)

	// Create Fs
	fs := testfs.NewMemoryFsFrom(inputDir)
	envs := env.Empty()
	envs.Set(`LOCAL_PROJECT_ID`, `12345`)
	testhelper.ReplaceEnvsDir(fs, `/`, envs)

	// Dependencies
	d := testdeps.New()
	d.SetFs(fs)
	d.UseMockedSchedulerApi()
	storageApi, httpTransport := d.UseMockedStorageApi()
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
	projectState, err := d.ProjectState(loadState.Options{LoadLocalState: true, IgnoreNotFoundErr: true})
	assert.NoError(t, err)

	// Assert state before
	assert.Equal(t, tc.untrackedPaths, projectState.UntrackedPaths())
	for _, objectState := range tc.expectedStates {
		_, found := projectState.Get(objectState.Key())
		assert.Falsef(t, found, `%s should not exists`, objectState.Desc())
	}
	for _, key := range tc.expectedMissing {
		_, found := projectState.Manifest().GetRecord(key)
		assert.Truef(t, found, `%s should exists`, key.Desc())
	}

	// Get plan
	plan, err := NewPlan(projectState.State())
	assert.NoError(t, err)

	// Delete callbacks for easier comparison (we only check callbacks result)
	for _, action := range plan.actions {
		if a, ok := action.(*newObjectAction); ok {
			a.OnPersist = nil
		}
	}

	// Assert plan
	assert.Equalf(t, tc.expectedPlan, plan.actions, `unexpected persist plan`)

	// Invoke
	plan, err = NewPlan(projectState.State()) // plan with callbacks
	assert.NoError(t, err)
	assert.NoError(t, plan.Invoke(d.Logger(), storageApi, projectState.State()))

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
		_, found := projectState.Manifest().GetRecord(key)
		assert.Falsef(t, found, `%s should not exists`, key.Desc())
	}
}
