package diff

import (
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/sort"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDiff_OnlyInA(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branch := &model.Branch{BranchKey: model.BranchKey{BranchId: 123}}
	A.MustAdd(branch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultOnlyInA, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())

	assert.Same(t, branch, result.A.Object)
	assert.Nil(t, result.B.Object)
}

func TestDiff_OnlyInB(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branch := &model.Branch{BranchKey: model.BranchKey{BranchId: 123}}
	B.MustAdd(branch)

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	result := results.Results[0]
	assert.Equal(t, ResultOnlyInB, result.State)
	assert.True(t, result.ChangedFields.IsEmpty())

	assert.Nil(t, result.A.Object)
	assert.Same(t, branch, result.B.Object)
}

func TestDiff_Equal(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{BranchId: 123}

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

	branchKey := model.BranchKey{BranchId: 123}

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

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}

	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "A", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "B", // no diff:"true" tag
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)
	for _, result := range results.Results {
		assert.Equal(t, ResultEqual, result.State)
		assert.True(t, result.ChangedFields.IsEmpty())
	}
}

func TestDiff_NotEqualConfig(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}

	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "A", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "B", // no diff:"true" tag
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

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}

	A.MustAdd(&model.Branch{BranchKey: branchKey})
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

	B.MustAdd(&model.Branch{BranchKey: branchKey})
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
	assert.Equal(t, []string{`foo.bar`}, result2.ChangedFields.Get(`configuration`).Paths())
}

func TestDiff_Relations(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	targetKey := fixtures.MockedKey{Id: `123`}
	assert.NoError(t, d.naming.Attach(targetKey, model.NewAbsPath("", "path/to/target")))
	spew.Dump(d.naming.PathByKey(targetKey))

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
				OtherSide: targetKey,
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
relations:
  - manifest side relation mocked key "bar"
  + manifest side relation mocked key "foo"
  - api side relation mocked key "002"
  + api side relation "path/to/target"
`
	assert.Equal(t, strings.Trim(expected, "\n"), result.String())
	assert.Equal(t, []string{"InApi", "InManifest"}, result.ChangedFields.Get("relations").Paths()) // see model.RelationsBySide
}

func TestDiff_Transformation(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: `keboola.python-transformation-v2`, ConfigId: `456`}
	transformationKey := model.TransformationKey{ConfigKey: configKey}
	block1Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 0}
	block2Key := model.BlockKey{TransformationKey: transformationKey, BlockIndex: 1}
	code1Key := model.CodeKey{BlockKey: block1Key, CodeIndex: 0}
	code2Key := model.CodeKey{BlockKey: block2Key, CodeIndex: 0}

	assert.NoError(t, d.naming.Attach(block1Key, model.NewAbsPath(`branch/config/blocks`, `001-my-block-1`)))
	assert.NoError(t, d.naming.Attach(block2Key, model.NewAbsPath(`branch/config/blocks`, `002-my-block-2`)))
	assert.NoError(t, d.naming.Attach(code1Key, model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-1`)))
	assert.NoError(t, d.naming.Attach(code2Key, model.NewAbsPath(`branch/config/blocks/001-block-1`, `001-code-2`)))

	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{ConfigKey: configKey})
	A.MustAdd(&model.Transformation{TransformationKey: transformationKey})
	A.MustAdd(&model.Block{BlockKey: block1Key, Name: "My block"})
	A.MustAdd(&model.Code{
		CodeKey: code1Key,
		Name:    "Code 1",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 1;"},
			model.StaticScript{Value: "SELECT 2;"},
			model.StaticScript{Value: "SELECT 3;"},
		},
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{ConfigKey: configKey})
	B.MustAdd(&model.Transformation{TransformationKey: transformationKey})
	B.MustAdd(&model.Block{
		BlockKey: block1Key,
		Name:     "Block 1",
	})
	B.MustAdd(&model.Block{
		BlockKey: block2Key,
		Name:     "Block 2",
	})
	B.MustAdd(&model.Code{
		CodeKey: code1Key,
		Name:    "Code 1",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 1;"},
		},
	})
	B.MustAdd(&model.Code{
		CodeKey: code2Key,
		Name:    "Code 2",
		Scripts: model.Scripts{
			model.StaticScript{Value: "SELECT 2;"},
		},
	})

	results, err := d.diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	//s := spew.NewDefaultConfig()
	//s.DisableMethods = true

	//for _, r := range results.Results {
	//	if r.State != ResultEqual {
	//		s.Dump(r.Key.String())
	//		for _, ch := range r.ChangedFields.All() {
	//			fmt.Println(ch.Diff())
	//		}
	//	}
	//}

	result1 := results.Results[0] // branch
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())

	result2 := results.Results[1] // config
	assert.Equal(t, ResultNotEqual, result2.State)

	expected := `

`
	assert.Equal(t, strings.Trim(expected, "\n"), result2.String())
}

//func TestDiff_SharedCode(t *testing.T) {
//	t.Parallel()
//	A, B, d := newDiffer()
//
//	targetComponentId := model.ComponentId("keboola.snowflake-transformation")
//	branchKey := model.BranchKey{BranchId: 123}
//	configKey := model.ConfigKey{BranchId: 123, ComponentId: model.SharedCodeComponentId, ConfigId: `456`}
//	configRowKey := model.ConfigRowKey{BranchId: 123, ComponentId: model.SharedCodeComponentId, ConfigId: `456`, ConfigRowId: `789`}
//
//	A.MustAdd(&model.Branch{BranchKey: branchKey})
//	A.MustAdd(&model.SharedCodeRow{
//		SharedCodeKey: model.SharedCodeKey{Parent: configRowKey},
//		Target:        targetComponentId,
//		Scripts: model.Scripts{
//			model.StaticScript{Value: "SELECT 1;"},
//			model.StaticScript{Value: "SELECT 2;"},
//			model.StaticScript{Value: "SELECT 3;"},
//		},
//	})
//
//	B.MustAdd(&model.Branch{BranchKey: branchKey})
//	B.MustAdd(&model.Config{ConfigKey: configKey})
//	B.MustAdd(&model.SharedCodeRow{
//		SharedCodeKey: model.SharedCodeKey{Parent: configRowKey},
//		Scripts: model.Scripts{
//			model.StaticScript{Value: "SELECT 4;"},
//			model.StaticScript{Value: "SELECT 3;"},
//		},
//	})
//
//	results, err := d.diff(A, B)
//	assert.NoError(t, err)
//	assert.Len(t, results.Results, 3)
//
//	result1 := results.Results[0] // branch
//	assert.Equal(t, ResultEqual, result1.State)
//	assert.True(t, result1.ChangedFields.IsEmpty())
//
//	result2 := results.Results[1] // config
//	assert.Equal(t, ResultEqual, result2.State)
//	assert.True(t, result2.ChangedFields.IsEmpty())
//
//	result3 := results.Results[2]
//	assert.Equal(t, ResultNotEqual, result3.State)
//
//	expected := `
//sharedCode:
//  - SELECT 1;
//  + SELECT 4;
//
//  - SELECT 2;
//  -
//    SELECT 3;
//`
//	assert.Equal(t, strings.Trim(expected, "\n"), result3.String())
//}

//func TestDiff_Orchestration(t *testing.T) {
//	t.Parallel()
//	A, B, d := newDiffer()
//
//	branchKey := model.BranchKey{Id: 123}
//	configKey := model.ConfigKey{BranchId: 123, ComponentId: model.OrchestratorComponentId, Id: `456`}
//	orchestrationKey := model.OrchestrationKey{Parent: configKey}
//	phase1Key := model.PhaseKey{Parent: orchestrationKey, Index: 0}
//	phase2Key := model.PhaseKey{Parent: orchestrationKey, Index: 1}
//	task1Key := model.TaskKey{Parent: phase1Key, Index: 0}
//	task2Key := model.TaskKey{Parent: phase1Key, Index: 1}
//	task3Key := model.TaskKey{Parent: phase1Key, Index: 0}
//	target1Key := model.ConfigKey{BranchId: 123, ComponentId: `foo.bar1`, Id: `123`}
//	target3Key := model.ConfigKey{BranchId: 123, ComponentId: `foo.bar3`, Id: `123`}
//
//	d.naming.MustAttach(phase1Key, model.NewAbsPath(`branch/other/orchestrator/phases`, `001-phase`))
//	d.naming.MustAttach(phase2Key, model.NewAbsPath(`branch/other/orchestrator/phases`, `002-phase`))
//	d.naming.MustAttach(task1Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-1`))
//	d.naming.MustAttach(task2Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `002-task-2`))
//	d.naming.MustAttach(task3Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-3`))
//	d.naming.MustAttach(target1Key, model.NewAbsPath("branch", "extractor/foo.bar1/123"))
//	d.naming.MustAttach(target3Key, model.NewAbsPath("branch", "extractor/foo.bar3/123"))
//
//	A.MustAdd(&model.Branch{BranchKey: branchKey})
//	A.MustAdd(&model.Config{ConfigKey: configKey})
//	A.MustAdd(&model.Orchestration{
//		OrchestrationKey: orchestrationKey,
//		Phases: []*model.Phase{
//			{
//				PhaseKey:  phase1Key,
//				DependsOn: []model.PhaseKey{},
//				Name:      `Phase`,
//				Content: orderedmap.FromPairs([]orderedmap.Pair{
//					{Key: `foo`, Value: `bar`},
//				}),
//				Tasks: []*model.Task{
//					{
//						TaskKey:     task3Key,
//						Name:        `Task 3`,
//						ComponentId: `foo.bar3`,
//						ConfigId:    `123`,
//						Content: orderedmap.FromPairs([]orderedmap.Pair{
//							{
//								Key: `task`,
//								Value: orderedmap.FromPairs([]orderedmap.Pair{
//									{Key: `mode`, Value: `run`},
//								}),
//							},
//							{Key: `continueOnFailure`, Value: false},
//							{Key: `enabled`, Value: true},
//						}),
//					},
//				},
//			},
//			{
//				PhaseKey:  phase2Key,
//				DependsOn: []model.PhaseKey{},
//				Name:      `New Phase`,
//				Content: orderedmap.FromPairs([]orderedmap.Pair{
//					{Key: `foo`, Value: `bar`},
//				}),
//			},
//		},
//	})
//
//	B.MustAdd(&model.Branch{BranchKey: branchKey})
//	B.MustAdd(&model.Config{
//		ConfigKey: configKey,
//	})
//	B.MustAdd(&model.Orchestration{
//		OrchestrationKey: orchestrationKey,
//		Phases: []*model.Phase{
//			{
//				PhaseKey:  phase1Key,
//				DependsOn: []model.PhaseKey{},
//				Name:      `Phase`,
//				Content: orderedmap.FromPairs([]orderedmap.Pair{
//					{Key: `foo`, Value: `bar`},
//				}),
//				Tasks: []*model.Task{
//					{
//						TaskKey:     task1Key,
//						Name:        `Task 1`,
//						ComponentId: `foo.bar1`,
//						ConfigId:    `123`,
//						Content: orderedmap.FromPairs([]orderedmap.Pair{
//							{
//								Key: `task`,
//								Value: orderedmap.FromPairs([]orderedmap.Pair{
//									{Key: `mode`, Value: `run`},
//								}),
//							},
//							{Key: `continueOnFailure`, Value: false},
//							{Key: `enabled`, Value: true},
//						}),
//					},
//					{
//						TaskKey:     task2Key,
//						Name:        `Task 2`,
//						ComponentId: `foo.bar2`,
//						ConfigId:    `123`,
//						Content: orderedmap.FromPairs([]orderedmap.Pair{
//							{
//								Key: `task`,
//								Value: orderedmap.FromPairs([]orderedmap.Pair{
//									{Key: `mode`, Value: `run`},
//								}),
//							},
//							{Key: `continueOnFailure`, Value: false},
//							{Key: `enabled`, Value: false},
//						}),
//					},
//				},
//			},
//		},
//	})
//
//	results, err := d.diff(A, B)
//	assert.NoError(t, err)
//	assert.Len(t, results.Results, 2)
//
//	result1 := results.Results[0] // branch
//	assert.Equal(t, ResultEqual, result1.State)
//	assert.True(t, result1.ChangedFields.IsEmpty())
//
//	result2 := results.Results[1] // config
//	assert.Equal(t, ResultNotEqual, result2.State)
//
//	expected := `
//orchestration:
//  001-phase:
//      #  001 Phase
//      depends on phases: []
//      {
//        "foo": "bar"
//      }
//    - ## 001 Task 3
//    - >> extractor/foo.bar3/123
//    + ## 001 Task 1
//    + >> extractor/foo.bar1/123
//      {
//        "task": {
//          "mode": "run"
//        },
//      ...
//    + ## 002 Task 2
//    + >> config "branch:123/component:foo.bar2/config:123"
//    + {
//    +   "task": {
//    +     "mode": "run"
//    +   },
//    +   "continueOnFailure": false,
//    +   "enabled": false
//    + }
//- 002-phase:
//-   #  002 New Phase
//-   depends on phases: []
//-   {
//-     "foo": "bar"
//-   }
//`
//	assert.Equal(t, strings.Trim(expected, "\n"), result2.String())
//}

func TestDiff_Map(t *testing.T) {
	t.Parallel()
	A, B, d := newDiffer()

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: model.OrchestratorComponentId, ConfigId: `456`}

	A.MustAdd(&model.Branch{BranchKey: branchKey})
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

	B.MustAdd(&model.Branch{BranchKey: branchKey})
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
	assert.Len(t, results.Results, 2)

	result1 := results.Results[0] // branch
	assert.Equal(t, ResultEqual, result1.State)
	assert.True(t, result1.ChangedFields.IsEmpty())

	result2 := results.Results[1] // config
	assert.Equal(t, ResultNotEqual, result2.State)

	expected := `
configuration:
  foo.bar:
    - {
    -   "baz": {
    -     "key": "value"
    -   }
    - }
    + "value"
+ key:
+   value
`
	assert.Equal(t, strings.Trim(expected, "\n"), result2.String())
}

func TestResults_Format(t *testing.T) {
	t.Parallel()
	changedFields := model.NewChangedFields()
	changedFields.Add("xyz").SetDiff(`diff 1`)
	changedFields.Add("123").SetDiff(`diff 2`)
	changedFields.Add("abc").SetDiff(`diff 3`)
	result := &Result{Key: fixtures.MockedKey{Id: "id"}, ChangedFields: changedFields, State: ResultNotEqual}
	results := &Results{Results: []*Result{result}}
	output := strings.Join(results.Format(naming.NewRegistry(), true), "\n")

	expected := `
* K mocked key "id"
  123:
  diff 2
  abc:
  diff 3
  xyz:
  diff 1
`
	assert.Equal(t, strings.Trim(expected, "\n"), output)
}

func newDiffer() (A, B model.Objects, d *differ) {
	namingReg := naming.NewRegistry()
	sorter := sort.NewPathSorter(namingReg)
	A = state.NewCollection(sorter)
	B = state.NewCollection(sorter)
	return A, B, &differ{naming: namingReg}
}
