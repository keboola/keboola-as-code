package diff

import (
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func TestDiffOnlyInLocal(t *testing.T) {
	t.Parallel()
	projectState := newProjectState(t)
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
	projectState := newProjectState(t)
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
	projectState := newProjectState(t)
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
	projectState := newProjectState(t)
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
	projectState := newProjectState(t)

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
		ComponentId: "foo.bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:   configKey,
			Name:        "config name",
			Description: "description",
		},
		Remote: &model.Config{
			ConfigKey:   configKey,
			Name:        "config name",
			Description: "description",
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
	projectState := newProjectState(t)

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
		ComponentId: "foo.bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:   configKey,
			Name:        "name",
			Description: "description",
		},
		Remote: &model.Config{
			ConfigKey:   configKey,
			Name:        "changed",
			Description: "changed",
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
	projectState := newProjectState(t)

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
		ComponentId: "foo.bar",
		Id:          "456",
	}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local: &model.Config{
			ConfigKey:   configKey,
			Name:        "name",
			Description: "description",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "bar", Value: "456"},
					}),
				},
			}),
		},
		Remote: &model.Config{
			ConfigKey:   configKey,
			Name:        "name",
			Description: "description",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
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
	projectState := newProjectState(t)

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

func TestDiffTransformation(t *testing.T) {
	t.Parallel()
	projectState := newProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `keboola.python-transformation-v2`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "config"),
			},
		},
		Local: &model.Config{
			SharedCode: &model.SharedCodeConfig{
				Target: storageapi.ComponentID(`12345`),
			},
			Transformation: &model.Transformation{
				Blocks: []*model.Block{
					{
						Name: "My block",
						Codes: model.Codes{
							{
								Name: "Code 1",
								Scripts: model.Scripts{
									model.StaticScript{Value: "SELECT 1;"},
									model.StaticScript{Value: "SELECT 2;"},
									model.StaticScript{Value: "SELECT 3;"},
								},
								AbsPath: model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-1`),
							},
						},
						AbsPath: model.NewAbsPath(`branch/config/blocks`, `001-my-block`),
					},
				},
			},
		},
		Remote: &model.Config{
			Transformation: &model.Transformation{
				Blocks: []*model.Block{
					{
						Name: "Block 1",
						Codes: model.Codes{
							{
								Name: "Code 1",
								Scripts: model.Scripts{
									model.StaticScript{Value: "SELECT 1;"},
								},
							},
						},
						AbsPath: model.NewAbsPath(`branch/config/blocks`, `001-block-1`),
					},
					{
						Name: "Block 2",
						Codes: model.Codes{
							{
								Name: "Code 2",
								Scripts: model.Scripts{
									model.StaticScript{Value: "SELECT 2;"},
								},
							},
						},
						AbsPath: model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-1`),
					},
				},
			},
		},
	}
	assert.NoError(t, projectState.Set(configState))

	// Transformation
	differ := NewDiffer(projectState)
	reporter := differ.diffValues(configState, configState.Remote.Transformation, configState.Local.Transformation)
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

	// SharedCode link
	reporter = differ.diffValues(configState, configState.Remote.SharedCode, configState.Local.SharedCode)
	expected = `
  - (null)
  + 12345
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
}

func TestDiffSharedCode(t *testing.T) {
	t.Parallel()
	projectState := newProjectState(t)

	// Object state
	configRowKey := model.ConfigRowKey{BranchId: 123, ComponentId: storageapi.SharedCodeComponentID, Id: `456`}
	configRowState := &model.ConfigRowState{
		ConfigRowManifest: &model.ConfigRowManifest{
			ConfigRowKey: configRowKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch/config", "row"),
			},
		},
		Local: &model.ConfigRow{
			SharedCode: &model.SharedCodeRow{
				Target: storageapi.ComponentID(`keboola.snowflake-transformation`),
				Scripts: model.Scripts{
					model.StaticScript{Value: "SELECT 1;"},
					model.StaticScript{Value: "SELECT 2;"},
					model.StaticScript{Value: "SELECT 3;"},
				},
			},
		},
		Remote: &model.ConfigRow{
			SharedCode: &model.SharedCodeRow{
				Target: storageapi.ComponentID(`keboola.snowflake-transformation`),
				Scripts: model.Scripts{
					model.StaticScript{Value: "SELECT 4;"},
					model.StaticScript{Value: "SELECT 3;"},
				},
			},
		},
	}
	assert.NoError(t, projectState.Set(configRowState))

	// Transformation
	differ := NewDiffer(projectState)
	reporter := differ.diffValues(configRowState, configRowState.Remote.SharedCode, configRowState.Local.SharedCode)
	expected := `
  - SELECT 4;
  + SELECT 1;
  
  + SELECT 2;
  + 
    SELECT 3;
`
	assert.Equal(t, strings.Trim(expected, "\n"), reporter.String())
}

func TestDiffOrchestration(t *testing.T) {
	t.Parallel()
	projectState := newProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: storageapi.OrchestratorComponentID, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "other/orchestrator"),
			},
		},
		Local: &model.Config{
			Orchestration: &model.Orchestration{
				Phases: []*model.Phase{
					{
						PhaseKey: model.PhaseKey{
							BranchId:    123,
							ComponentId: storageapi.OrchestratorComponentID,
							ConfigId:    `456`,
							Index:       0,
						},
						AbsPath:   model.NewAbsPath(`branch/other/orchestrator/phases`, `001-phase`),
						DependsOn: []model.PhaseKey{},
						Name:      `Phase`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: `foo`, Value: `bar`},
						}),
						Tasks: []*model.Task{
							{
								TaskKey: model.TaskKey{
									PhaseKey: model.PhaseKey{
										BranchId:    123,
										ComponentId: storageapi.OrchestratorComponentID,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 0,
								},
								AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-3`),
								Name:        `Task 3`,
								ComponentId: `foo.bar3`,
								ConfigId:    `123`,
								ConfigPath:  `branch/extractor/foo.bar3/123`,
								Content: orderedmap.FromPairs([]orderedmap.Pair{
									{
										Key: `task`,
										Value: orderedmap.FromPairs([]orderedmap.Pair{
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
							ComponentId: storageapi.OrchestratorComponentID,
							ConfigId:    `456`,
							Index:       1,
						},
						AbsPath:   model.NewAbsPath(`branch/other/orchestrator/phases`, `002-phase`),
						DependsOn: []model.PhaseKey{},
						Name:      `New Phase`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
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
							ComponentId: storageapi.OrchestratorComponentID,
							ConfigId:    `456`,
							Index:       0,
						},
						AbsPath:   model.NewAbsPath(`branch/other/orchestrator/phases`, `001-phase`),
						DependsOn: []model.PhaseKey{},
						Name:      `Phase`,
						Content: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: `foo`, Value: `bar`},
						}),
						Tasks: []*model.Task{
							{
								TaskKey: model.TaskKey{
									PhaseKey: model.PhaseKey{
										BranchId:    123,
										ComponentId: storageapi.OrchestratorComponentID,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 0,
								},
								AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-1`),
								Name:        `Task 1`,
								ComponentId: `foo.bar1`,
								ConfigId:    `123`,
								ConfigPath:  `branch/extractor/foo.bar1/config123`,
								Content: orderedmap.FromPairs([]orderedmap.Pair{
									{
										Key: `task`,
										Value: orderedmap.FromPairs([]orderedmap.Pair{
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
										ComponentId: storageapi.OrchestratorComponentID,
										ConfigId:    `456`,
										Index:       0,
									},
									Index: 1,
								},
								AbsPath:     model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `002-task-2`),
								Name:        `Task 2`,
								ComponentId: `foo.bar2`,
								ConfigId:    `789`,
								Content: orderedmap.FromPairs([]orderedmap.Pair{
									{
										Key: `task`,
										Value: orderedmap.FromPairs([]orderedmap.Pair{
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
	projectState := newProjectState(t)

	// Object state
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `keboola.python-transformation-v2`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
			Paths: model.Paths{
				AbsPath: model.NewAbsPath("branch", "config"),
			},
		},
		Local: &model.Config{
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
						{
							Key: "bar",
							Value: orderedmap.FromPairs([]orderedmap.Pair{
								{
									Key: "baz",
									Value: orderedmap.FromPairs([]orderedmap.Pair{
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
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{
					Key: "foo",
					Value: orderedmap.FromPairs([]orderedmap.Pair{
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

func TestResults_Format(t *testing.T) {
	t.Parallel()
	changedFields := model.NewChangedFields()
	changedFields.Add("xyz").SetDiff(`diff 1`)
	changedFields.Add("123").SetDiff(`diff 2`)
	changedFields.Add("abc").SetDiff(`diff 3`)
	objectState := &fixtures.MockedObjectState{MockedManifest: &fixtures.MockedManifest{}}
	result := &Result{ChangedFields: changedFields, State: ResultNotEqual, ObjectState: objectState}
	results := &Results{Results: []*Result{result}}
	output := strings.Join(results.Format(true), "\n")

	expected := `* K test
  123:
  diff 2
  abc:
  diff 3
  xyz:
  diff 1`
	assert.Equal(t, expected, output)
}

func newProjectState(t *testing.T) *state.State {
	t.Helper()
	d := dependencies.NewTestContainer()
	d.SetLocalProject(project.NewWithManifest(d.Fs(), projectManifest.New(12345, `foo.bar`), d))
	d.UseMockedStorageApi()
	d.UseMockedSchedulerApi()
	projectState, err := d.LocalProjectState(loadState.Options{
		LoadLocalState:  false,
		LoadRemoteState: false,
	})
	assert.NoError(t, err)
	return projectState.State()
}
