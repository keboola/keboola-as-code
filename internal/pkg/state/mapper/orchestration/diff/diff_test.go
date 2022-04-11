package diff_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	. "github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/diff/format"
	orchestrationDiff "github.com/keboola/keboola-as-code/internal/pkg/state/mapper/orchestration/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDiff_Orchestration(t *testing.T) {
	t.Parallel()
	A, B, d := state.NewCollection(), state.NewCollection(), NewDiffer(orchestrationDiff.Option())

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: model.OrchestratorComponentId, ConfigId: `456`}
	orchestrationKey := model.OrchestrationKey{ConfigKey: configKey}
	phase1Key := model.PhaseKey{OrchestrationKey: orchestrationKey, PhaseIndex: 0}
	phase2Key := model.PhaseKey{OrchestrationKey: orchestrationKey, PhaseIndex: 1}
	task1Key := model.TaskKey{PhaseKey: phase1Key, TaskIndex: 0}
	task2Key := model.TaskKey{PhaseKey: phase1Key, TaskIndex: 1}
	task3Key := model.TaskKey{PhaseKey: phase1Key, TaskIndex: 0}
	target1Key := model.ConfigKey{BranchKey: branchKey, ComponentId: `foo.bar1`, ConfigId: `123`}
	target2Key := model.ConfigKey{BranchKey: branchKey, ComponentId: `foo.bar2`, ConfigId: `123`}
	target3Key := model.ConfigKey{BranchKey: branchKey, ComponentId: `foo.bar3`, ConfigId: `123`}

	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{ConfigKey: target3Key})
	A.MustAdd(&model.Config{ConfigKey: configKey})
	A.MustAdd(&model.Orchestration{OrchestrationKey: orchestrationKey})
	A.MustAdd(&model.Phase{
		PhaseKey:  phase1Key,
		DependsOn: []model.PhaseKey{},
		Name:      `Phase`,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: `foo`, Value: `bar`},
		}),
	})
	A.MustAdd(&model.Phase{
		PhaseKey:  phase2Key,
		DependsOn: []model.PhaseKey{},
		Name:      `New Phase`,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: `foo`, Value: `bar`},
		}),
	})
	A.MustAdd(&model.Task{
		TaskKey:     task3Key,
		Name:        `Removed Task`,
		ComponentId: `foo.bar3`,
		ConfigId:    `123`,
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
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{ConfigKey: target1Key})
	B.MustAdd(&model.Config{ConfigKey: target2Key})
	B.MustAdd(&model.Config{
		ConfigKey: configKey,
	})
	B.MustAdd(&model.Orchestration{OrchestrationKey: orchestrationKey})
	B.MustAdd(&model.Phase{
		PhaseKey:  phase1Key,
		DependsOn: []model.PhaseKey{},
		Name:      `Phase`,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: `foo`, Value: `bar`},
		}),
	})
	B.MustAdd(&model.Task{
		TaskKey:     task1Key,
		Name:        `Task 1`,
		ComponentId: `foo.bar1`,
		ConfigId:    `123`,
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
	})
	B.MustAdd(&model.Task{
		TaskKey:     task2Key,
		Name:        `Task 2`,
		ComponentId: `foo.bar2`,
		ConfigId:    `123`,
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
	})

	// Do diff
	results, err := d.Diff(A, B)
	assert.NoError(t, err)

	// Config result state is ResultNotEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultNotEqual, result.State)
	assert.True(t, found)
	//assert.Equal(t, `description, name`, result2.ChangedFields.String())

	// Setup naming
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branchKey, model.NewAbsPath(``, `branch`))
	namingReg.MustAttach(configKey, model.NewAbsPath(`branch`, `other/orchestrator`))
	namingReg.MustAttach(orchestrationKey, model.NewAbsPath(`branch/other/orchestrator`, `phases`))
	namingReg.MustAttach(phase1Key, model.NewAbsPath(`branch/other/orchestrator/phases`, `001-phase`))
	namingReg.MustAttach(phase2Key, model.NewAbsPath(`branch/other/orchestrator/phases`, `002-phase`))
	namingReg.MustAttach(task1Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-1`))
	namingReg.MustAttach(task2Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `002-task-2`))
	namingReg.MustAttach(task3Key, model.NewAbsPath(`branch/other/orchestrator/phases/001-phase`, `001-task-3`))
	namingReg.MustAttach(target1Key, model.NewAbsPath("branch", "extractor/foo.bar1/123"))
	namingReg.MustAttach(target2Key, model.NewAbsPath("branch", "extractor/foo.bar2/123"))
	namingReg.MustAttach(target3Key, model.NewAbsPath("branch", "extractor/foo.bar3/123"))

	// Formatted result without details
	assert.Equal(t, strings.TrimLeft(`
+ C branch:123/component:foo.bar1/config:123
+ C branch:123/component:foo.bar2/config:123
- C branch:123/component:foo.bar3/config:123
* C branch:123/component:keboola.orchestrator/config:456 | changes: orchestration
`, "\n"), format.Format(results))

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
+ C branch:123/component:foo.bar1/config:123
+ C branch:123/component:foo.bar2/config:123
- C branch:123/component:foo.bar3/config:123
* C branch:123/component:keboola.orchestrator/config:456
    orchestration:
      # 001 Phase
      depends on phases: []
      {
        "foo": "bar"
      }
    - ## 001 Removed Task
    - >> branch:123/component:foo.bar3/config:123
    + ## 001 Task 1
    + >> branch:123/component:foo.bar1/config:123
      {
        "task": {
          "mode": "run"
        },
      ...
    - # 002 New Phase
    - depends on phases: []
    + ## 002 Task 2
    + >> branch:123/component:foo.bar2/config:123
      {
    -   "foo": "bar"
    +   "task": {
    +     "mode": "run"
    +   },
    +   "continueOnFailure": false,
    +   "enabled": false
      }
`, "\n"), format.Format(results, format.WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, strings.TrimLeft(`
+ C branch/extractor/foo.bar1/123
+ C branch/extractor/foo.bar2/123
- C branch/extractor/foo.bar3/123
* C branch/other/orchestrator | changes: orchestration
`, "\n"), format.Format(results, format.WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
+ C branch/extractor/foo.bar1/123
+ C branch/extractor/foo.bar2/123
- C branch/extractor/foo.bar3/123
* C branch/other/orchestrator
    orchestration:
      # 001 Phase
      depends on phases: []
      {
        "foo": "bar"
      }
    - ## 001 Removed Task
    - >> branch/extractor/foo.bar3/123
    + ## 001 Task 1
    + >> branch/extractor/foo.bar1/123
      {
        "task": {
          "mode": "run"
        },
      ...
    - # 002 New Phase
    - depends on phases: []
    + ## 002 Task 2
    + >> branch/extractor/foo.bar2/123
      {
    -   "foo": "bar"
    +   "task": {
    +     "mode": "run"
    +   },
    +   "continueOnFailure": false,
    +   "enabled": false
      }
`, "\n"), format.Format(results, format.WithNamingRegistry(namingReg), format.WithDetails()))
}
