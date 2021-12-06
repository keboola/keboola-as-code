package diff

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestDiffOnlyInLocal(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Local:          &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, projectState.Set(branchState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInLocal, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Local, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffOnlyInRemote(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote:         &model.Branch{BranchKey: branchKey},
	}
	assert.NoError(t, projectState.Set(branchState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInRemote, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Remote, result.ObjectState.RemoteState().(*model.Branch))
}

func TestDiffEqual(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
		Local: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
	}
	assert.NoError(t, projectState.Set(branchState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Remote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchState.Local, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffNotEqual(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)
	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
		Local: &model.Branch{
			BranchKey:   branchKey,
			Name:        "changed",
			Description: "description",
			IsDefault:   true,
		},
	}
	assert.NoError(t, projectState.Set(branchState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)
	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)
	assert.Equal(t, `isDefault, name`, result.ChangedFields.String())
	assert.Equal(t, "  - name\n  + changed", result.ChangedFields.Get("name").Diff())
	assert.Equal(t, "  - false\n  + true", result.ChangedFields.Get("isDefault").Diff())
	assert.Same(t, branchState.Remote, result.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchState.Local, result.ObjectState.LocalState().(*model.Branch))
}

func TestDiffEqualConfig(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote: &model.Branch{
			BranchKey:   branchKey,
			Name:        "branch name",
			Description: "description",
			IsDefault:   false,
		},
		Local: &model.Branch{
			BranchKey:   branchKey,
			Name:        "branch name",
			Description: "description",
			IsDefault:   false,
		},
	}
	assert.NoError(t, projectState.Set(branchState))

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:         configKey,
			Name:              "config name",
			Description:       "description",
			ChangeDescription: "remote", // no diff:"true" tag
		},
		Remote: &model.Config{
			ConfigKey:         configKey,
			Name:              "config name",
			Description:       "description",
			ChangeDescription: "local", // no diff:"true" tag
		},
	}
	assert.NoError(t, projectState.Set(configState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Remote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchState.Local, result1.ObjectState.LocalState().(*model.Branch))
	result2 := results.Results[1]
	assert.Equal(t, ResultEqual, result2.State)
	assert.True(t, result2.ChangedFields.IsEmpty())
	assert.Same(t, configState.Remote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configState.Local, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffNotEqualConfig(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
		Local: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
	}
	assert.NoError(t, projectState.Set(branchState))

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:         configKey,
			Name:              "name",
			Description:       "description",
			ChangeDescription: "remote", // no diff:"true" tag
		},
		Remote: &model.Config{
			ConfigKey:         configKey,
			Name:              "changed",
			Description:       "changed",
			ChangeDescription: "local", // no diff:"true" tag
		},
	}
	assert.NoError(t, projectState.Set(configState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Remote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchState.Local, result1.ObjectState.LocalState().(*model.Branch))

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `description, name`, result2.ChangedFields.String())
	assert.Same(t, configState.Remote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configState.Local, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffNotEqualConfigConfiguration(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	component := &model.Component{
		ComponentKey: model.ComponentKey{
			Id: "foo-bar",
		},
	}
	projectState.Components().Set(component)

	branchKey := model.BranchKey{Id: 123}
	branchState := &model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: branchKey},
		Remote: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
		Local: &model.Branch{
			BranchKey:   branchKey,
			Name:        "name",
			Description: "description",
			IsDefault:   false,
		},
	}
	assert.NoError(t, projectState.Set(branchState))

	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo-bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:         configKey,
			Name:              "name",
			Description:       "description",
			ChangeDescription: "remote", // no diff:"true" tag
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "bar", Value: "456"},
					}),
				},
			}),
		},
		Remote: &model.Config{
			ConfigKey:         configKey,
			Name:              "name",
			Description:       "description",
			ChangeDescription: "local", // no diff:"true" tag
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "bar", Value: "123"},
					}),
				},
			}),
		},
	}
	assert.NoError(t, projectState.Set(configState))

	d := NewDiffer(projectState)
	results, err := d.Diff()
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	assert.Same(t, branchState.Remote, result1.ObjectState.RemoteState().(*model.Branch))
	assert.Same(t, branchState.Local, result1.ObjectState.LocalState().(*model.Branch))

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `configuration`, result2.ChangedFields.String())
	assert.Equal(t, `foo.bar`, result2.ChangedFields.Get(`configuration`).Paths())
	assert.Same(t, configState.Remote, result2.ObjectState.RemoteState().(*model.Config))
	assert.Same(t, configState.Local, result2.ObjectState.LocalState().(*model.Config))
}

func TestDiffRelations(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	// Target object
	targetKey := fixtures.MockedKey{
		Id: `123`,
	}
	targetState := &fixtures.MockedObjectState{
		MockedManifest: &fixtures.MockedManifest{
			MockedKey: targetKey,
			PathValue: `path/to/target`,
		},
	}
	assert.NoError(t, projectState.Set(targetState))

	objectKey := fixtures.MockedKey{
		Id: `345`,
	}
	objectState := &fixtures.MockedObjectState{
		MockedManifest: &fixtures.MockedManifest{
			MockedKey: objectKey,
			PathValue: `path/to/object`,
		},
		Local: &fixtures.MockedObject{
			MockedKey: objectKey,
			Relations: model.Relations{
				&fixtures.MockedApiSideRelation{
					OtherSide: fixtures.MockedKey{Id: `001`},
				},
				&fixtures.MockedApiSideRelation{
					OtherSide: fixtures.MockedKey{Id: `002`},
				},
				&fixtures.MockedManifestSideRelation{
					OtherSide: fixtures.MockedKey{Id: `bar`},
				},
			},
		},
		Remote: &fixtures.MockedObject{
			MockedKey: objectKey,
			Relations: model.Relations{
				&fixtures.MockedApiSideRelation{
					OtherSide: fixtures.MockedKey{Id: `123`},
				},
				&fixtures.MockedApiSideRelation{
					OtherSide: fixtures.MockedKey{Id: `001`},
				},
				&fixtures.MockedManifestSideRelation{
					OtherSide: fixtures.MockedKey{Id: `foo`},
				},
			},
		},
	}
	assert.NoError(t, projectState.Set(objectState))

	differ := NewDiffer(projectState)
	reporter := differ.diffValues(objectState, objectState.Remote.Relations, objectState.Local.Relations)
	expected := `
  - manifest side relation mocked key "foo"
  + manifest side relation mocked key "bar"
  - api side relation "path/to/target"
  + api side relation mocked key "002"
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
	assert.Equal(t, []string{"InManifest", "InApi"}, reporter.Paths()) // see model.RelationsBySide
}

func TestDiffBlocks(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `keboola.python-transformation-v2`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject("branch", "config"),
			},
		},
		Local: &model.Config{
			Blocks: model.Blocks{
				{
					Name: "My block",
					Codes: model.Codes{
						{
							Name: "Code 1",
							Scripts: []string{
								"SELECT 1;",
								"SELECT 2;",
								"SELECT 3;",
							},
							PathInProject: model.NewPathInProject(`branch/config/blocks/001-block-1`, `001-code-1`),
						},
					},
					PathInProject: model.NewPathInProject(`branch/config/blocks`, `001-my-block`),
				},
			},
		},
		Remote: &model.Config{
			Blocks: model.Blocks{
				{
					Name: "Block 1",
					Codes: model.Codes{
						{
							Name: "Code 1",
							Scripts: []string{
								"SELECT 1;",
							},
						},
					},
					PathInProject: model.NewPathInProject(`branch/config/blocks`, `001-block-1`),
				},
				{
					Name: "Block 2",
					Codes: model.Codes{
						{
							Name: "Code 2",
							Scripts: []string{
								"SELECT 2;",
							},
						},
					},
					PathInProject: model.NewPathInProject(`branch/config/blocks/001-block-1`, `001-code-1`),
				},
			},
		},
	}
	assert.NoError(t, projectState.Set(configState))

	differ := NewDiffer(projectState)
	reporter := differ.diffValues(configState, configState.Remote.Blocks, configState.Local.Blocks)
	expected := `
  blocks/001-my-block:
    - #  Block 1
    + #  My block
      ## Code 1
      SELECT 1;
    + SELECT 2;
    + SELECT 3;
- blocks/001-block-1/001-code-1:
-   #  Block 2
-   ## Code 2
-   SELECT 2;
-   
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
}

func TestDiffOrchestration(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: model.OrchestratorComponentId, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject("branch", "other/orchestrator"),
			},
		},
		Local: &model.Config{
			Orchestration: &model.Orchestration{
				Phases: []*model.Phase{
					{
						PhaseKey: model.PhaseKey{
							BranchId:    123,
							ComponentId: model.OrchestratorComponentId,
							ConfigId:    `456`,
							Index:       0,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases`, `001-phase`),
						DependsOn:     []model.PhaseKey{},
						Name:          `Phase`,
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{Key: `foo`, Value: `bar`},
						}),
						Tasks: []*model.Task{
							{
								TaskKey: model.TaskKey{
									PhaseKey: model.PhaseKey{
										BranchId:    123,
										ComponentId: model.OrchestratorComponentId,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 0,
								},
								PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/001-phase`, `001-task-3`),
								Name:          `Task 3`,
								ComponentId:   `foo.bar3`,
								ConfigId:      `123`,
								ConfigPath:    `branch/extractor/foo.bar3/123`,
								Content: utils.PairsToOrderedMap([]utils.Pair{
									{
										Key: `task`,
										Value: utils.PairsToOrderedMap([]utils.Pair{
											{Key: `mode`, Value: `run`},
										}),
									},
									{Key: `continueOnFailure`, Value: false},
									{Key: `enabled`, Value: true},
								}),
							},
						},
					},
					{
						PhaseKey: model.PhaseKey{
							BranchId:    123,
							ComponentId: model.OrchestratorComponentId,
							ConfigId:    `456`,
							Index:       1,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases`, `002-phase`),
						DependsOn:     []model.PhaseKey{},
						Name:          `New Phase`,
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{Key: `foo`, Value: `bar`},
						}),
					},
				},
			},
		},
		Remote: &model.Config{
			Orchestration: &model.Orchestration{
				Phases: []*model.Phase{
					{
						PhaseKey: model.PhaseKey{
							BranchId:    123,
							ComponentId: model.OrchestratorComponentId,
							ConfigId:    `456`,
							Index:       0,
						},
						PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases`, `001-phase`),
						DependsOn:     []model.PhaseKey{},
						Name:          `Phase`,
						Content: utils.PairsToOrderedMap([]utils.Pair{
							{Key: `foo`, Value: `bar`},
						}),
						Tasks: []*model.Task{
							{
								TaskKey: model.TaskKey{
									PhaseKey: model.PhaseKey{
										BranchId:    123,
										ComponentId: model.OrchestratorComponentId,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 0,
								},
								PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/001-phase`, `001-task-1`),
								Name:          `Task 1`,
								ComponentId:   `foo.bar1`,
								ConfigId:      `123`,
								ConfigPath:    `branch/extractor/foo.bar1/config123`,
								Content: utils.PairsToOrderedMap([]utils.Pair{
									{
										Key: `task`,
										Value: utils.PairsToOrderedMap([]utils.Pair{
											{Key: `mode`, Value: `run`},
										}),
									},
									{Key: `continueOnFailure`, Value: false},
									{Key: `enabled`, Value: true},
								}),
							},
							{
								TaskKey: model.TaskKey{
									PhaseKey: model.PhaseKey{
										BranchId:    123,
										ComponentId: model.OrchestratorComponentId,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 1,
								},
								PathInProject: model.NewPathInProject(`branch/other/orchestrator/phases/001-phase`, `002-task-2`),
								Name:          `Task 2`,
								ComponentId:   `foo.bar2`,
								ConfigId:      `789`,
								Content: utils.PairsToOrderedMap([]utils.Pair{
									{
										Key: `task`,
										Value: utils.PairsToOrderedMap([]utils.Pair{
											{Key: `mode`, Value: `run`},
										}),
									},
									{Key: `continueOnFailure`, Value: false},
									{Key: `enabled`, Value: false},
								}),
							},
						},
					},
				},
			},
		},
	}
	assert.NoError(t, projectState.Set(configState))

	differ := NewDiffer(projectState)
	reporter := differ.diffValues(configState, configState.Remote.Orchestration, configState.Local.Orchestration)
	expected := `
  phases/001-phase:
      #  001 Phase
      depends on phases: []
      {
        "foo": "bar"
      ...
    - ## 001 Task 1
    - >> branch/extractor/foo.bar1/config123
    + ## 001 Task 3
    + >> branch/extractor/foo.bar3/123
      {
        "task": {
          "mode": "run"
        },
      ...
    - ## 002 Task 2
    - >> branch:123/componentId:foo.bar2/configId:789
    - {
    -   "task": {
    -     "mode": "run"
    -   },
    -   "continueOnFailure": false,
    -   "enabled": false
    - }
+ phases/002-phase:
+   #  002 New Phase
+   depends on phases: []
+   {
+     "foo": "bar"
+   }
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
}

func TestDiffMap(t *testing.T) {
	t.Parallel()
	projectState := createProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `keboola.python-transformation-v2`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				PathInProject: model.NewPathInProject("branch", "config"),
			},
		},
		Local: &model.Config{
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{
							Key: "bar",
							Value: utils.PairsToOrderedMap([]utils.Pair{
								{
									Key: "baz",
									Value: utils.PairsToOrderedMap([]utils.Pair{
										{Key: "key", Value: "value"},
									}),
								},
							}),
						},
					}),
				},
			}),
		},
		Remote: &model.Config{
			Content: utils.PairsToOrderedMap([]utils.Pair{
				{
					Key: "foo",
					Value: utils.PairsToOrderedMap([]utils.Pair{
						{Key: "bar", Value: "value"},
					}),
				},
				{Key: "key", Value: "value"},
			}),
		},
	}
	assert.NoError(t, projectState.Set(configState))

	differ := NewDiffer(projectState)
	reporter := differ.diffValues(configState, configState.Remote.Content, configState.Local.Content)
	expected := `
  foo.bar:
    - "value"
    + {
    +   "baz": {
    +     "key": "value"
    +   }
    + }
- key:
-   value
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
}

func createProjectState(t *testing.T) *state.State {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	m, err := manifest.NewManifest(1, `foo.bar`, fs)
	if err != nil {
		assert.FailNow(t, err.Error())
	}

	storageApi, _, _ := testapi.NewMockedStorageApi()
	schedulerApi, _, _ := testapi.NewMockedSchedulerApi()
	options := state.NewOptions(m, storageApi, schedulerApi, context.Background(), logger)
	options.LoadLocalState = false
	options.LoadRemoteState = false
	s, _, localErr, remoteErr := state.LoadState(options)
	assert.NoError(t, localErr)
	assert.NoError(t, remoteErr)
	return s
}
