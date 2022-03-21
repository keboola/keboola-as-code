package diff

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/object"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDiff_OnlyInA(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branch := &model.Branch{BranchKey: model.BranchKey{Id: 123}}
	A.MustAdd(branch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultOnlyInA, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())

	assert.Same(t, branch, result.A.Object)
	assert.Nil(t, result.B)
}

func TestDiff_OnlyInB(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branch := &model.Branch{BranchKey: model.BranchKey{Id: 123}}
	A.MustAdd(branch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultOnlyInB, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())

	assert.Nil(t, result.A)
	assert.Same(t, branch, result.B.Object)
}

func TestDiff_Equal(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{Id: 123}

	aBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	A.MustAdd(aBranch)

	bBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	B.MustAdd(bBranch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())
	assert.Same(t, aBranch, result.A.Object.(*model.Branch))
	assert.Same(t, bBranch, result.B.Object.(*model.Branch))
}

func TestDiff_NotEqual(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{Id: 123}

	aBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	A.MustAdd(aBranch)

	bBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "changed",
		Description: "description",
		IsDefault:   true,
	}
	B.MustAdd(bBranch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)
	assert.Equal(t, `isDefault, name`, result.ChangedFields.String())
	assert.Equal(t, "  - name\n  + changed", result.ChangedFields.Get("name").Diff())
	assert.Equal(t, "  - false\n  + true", result.ChangedFields.Get("isDefault").Diff())
	assert.Same(t, aBranch, result.A.Object.(*model.Branch))
	assert.Same(t, bBranch, result.B.Object.(*model.Branch))
}

func TestDiff_EqualConfig(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{Id: 123}
	configKey := model.ConfigKey{BranchId: 123, ComponentId: "foo-bar", Id: "456"}

	A.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())
	result2 := results.Results[1]
	assert.Equal(t, ResultEqual, result2.State)
	assert.True(t, result2.ChangedFields.IsEmpty())
}

func TestDiff_NotEqualConfig(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{Id: 123}
	configKey := model.ConfigKey{BranchId: 123, ComponentId: "foo-bar", Id: "456"}

	A.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "local", // no diff:"true" tag
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `description, name`, result2.ChangedFields.String())
}

func TestDiff_NotEqualConfigConfiguration(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{Id: 123}
	configKey := model.ConfigKey{BranchId: 123, ComponentId: "foo-bar", Id: "456"}

	A.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "456"},
				}),
			},
		}),
	})

	B.MustAdd(&model.Branch{
		BranchKey:   branchKey,
		Name:        "branch name",
		Description: "description",
		IsDefault:   false,
	})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "123"},
				}),
			},
		}),
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0]
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())

	result2 := results.Results[1]
	assert.Equal(t, ResultNotEqual, result2.State)
	assert.Equal(t, `configuration`, result2.ChangedFields.String())
	assert.Equal(t, `foo.bar`, result2.ChangedFields.Get(`configuration`).Paths())
}

func TestDiff_Relations(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	objectKey := fixtures.MockedKey{Id: `345`}
	A.MustAdd(&fixtures.MockedObject{
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
	})
	B.MustAdd(&fixtures.MockedObject{
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
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	expected := `
  - manifest side relation mocked key "foo"
  + manifest side relation mocked key "bar"
  - api side relation "path/to/target"
  + api side relation mocked key "002"
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
	assert.Equal(t, []string{"InManifest", "InApi"}, result.ChangedFields.Get("relations").Paths()) // see model.RelationsBySide
}

func TestDiff_Transformation(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	configKey := model.ConfigKey{BranchId: 123, ComponentId: `keboola.python-transformation-v2`, Id: `456`}
	A.MustAdd(&model.Config{
		ConfigKey: configKey,
		SharedCode: &model.SharedCodeConfig{
			Target: model.ComponentId(`12345`),
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
	})
	B.MustAdd(&model.Config{
		ConfigKey: configKey,
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
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	expected := `
  x
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
}

func TestDiff_SharedCode(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	configRowKey := model.ConfigRowKey{BranchId: 123, ComponentId: model.SharedCodeComponentId, Id: `456`}
	A.MustAdd(&model.ConfigRow{
		ConfigRowKey: configRowKey,
		SharedCode: &model.SharedCodeRow{
			Target: model.ComponentId(`keboola.snowflake-transformation`),
			Scripts: model.Scripts{
				model.StaticScript{Value: "SELECT 1;"},
				model.StaticScript{Value: "SELECT 2;"},
				model.StaticScript{Value: "SELECT 3;"},
			},
		},
	})
	B.MustAdd(&model.ConfigRow{
		ConfigRowKey: configRowKey,
		SharedCode: &model.SharedCodeRow{
			Target: model.ComponentId(`keboola.snowflake-transformation`),
			Scripts: model.Scripts{
				model.StaticScript{Value: "SELECT 4;"},
				model.StaticScript{Value: "SELECT 3;"},
			},
		},
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	expected := `
  x
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
}

func TestDiff_Orchestration(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	configKey := model.ConfigKey{BranchId: 123, ComponentId: model.OrchestratorComponentId, Id: `456`}
	A.MustAdd(&model.Config{
		ConfigKey: configKey,
		Orchestration: &model.Orchestration{
			Phases: []*model.Phase{
				{
					PhaseKey: model.PhaseKey{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
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
									ComponentId: model.OrchestratorComponentId,
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
						ComponentId: model.OrchestratorComponentId,
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
	})
	B.MustAdd(&model.Config{
		ConfigKey: configKey,
		Orchestration: &model.Orchestration{
			Phases: []*model.Phase{
				{
					PhaseKey: model.PhaseKey{
						BranchId:    123,
						ComponentId: model.OrchestratorComponentId,
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
									ComponentId: model.OrchestratorComponentId,
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
									ComponentId: model.OrchestratorComponentId,
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
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	expected := `
  x
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
}

func TestDiff_Map(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	configKey := model.ConfigKey{BranchId: 123, ComponentId: model.OrchestratorComponentId, Id: `456`}
	A.MustAdd(&model.Config{
		ConfigKey: configKey,
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
	})

	B.MustAdd(&model.Config{
		ConfigKey: configKey,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "value"},
				}),
			},
			{Key: "key", Value: "value"},
		}),
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	expected := `
  x
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
}

func TestResults_Format(t *testing.T) {
	t.Parallel()
	changedFields := model.NewChangedFields()
	changedFields.Add("xyz").SetDiff(`diff 1`)
	changedFields.Add("123").SetDiff(`diff 2`)
	changedFields.Add("abc").SetDiff(`diff 3`)
	result := &Result{ChangedFields: changedFields, State: ResultNotEqual}
	results := &Results{Results: []*Result{result}}
	output := strings.Join(results.Format(naming.NewRegistry(), true), "\n")

	expected := `* K test
  123:
  diff 2
  abc:
  diff 3
  xyz:
  diff 1`
	assert.Equal(t, expected, output)
}

func newDiffer() (A, B model.Objects, d *differ) {
	namingReg := naming.NewRegistry()
	sorter := object.NewPathSorter(namingReg)
	A = object.NewCollection(sorter)
	B = object.NewCollection(sorter)
	return A, B, &differ{naming: namingReg}
}
