package model_test

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func TestAllObjectTypes(t *testing.T) {
	// Define keys
	branch1Key := BranchKey{BranchId: 1}
	branch2Key := BranchKey{BranchId: 2}
	branch3Key := BranchKey{BranchId: 3}
	config1Key := ConfigKey{BranchKey: branch1Key, ComponentId: "foo.bar", ConfigId: "4"}
	config2Key := ConfigKey{BranchKey: branch2Key, ComponentId: "foo.bar", ConfigId: "5"}
	config3Key := ConfigKey{BranchKey: branch3Key, ComponentId: "foo.bar", ConfigId: "6"}
	configSharedCodesKey := ConfigKey{BranchKey: branch3Key, ComponentId: SharedCodeComponentId, ConfigId: "7"}
	configRow1Key := ConfigRowKey{ConfigKey: config1Key, ConfigRowId: "8"}
	configRow2Key := ConfigRowKey{ConfigKey: config1Key, ConfigRowId: "9"}
	configRow3Key := ConfigRowKey{ConfigKey: config1Key, ConfigRowId: "10"}
	transformationKey := TransformationKey{ConfigKey: config2Key}
	block1Key := BlockKey{TransformationKey: transformationKey, BlockIndex: 0}
	block2Key := BlockKey{TransformationKey: transformationKey, BlockIndex: 1}
	block3Key := BlockKey{TransformationKey: transformationKey, BlockIndex: 2}
	code1Key := CodeKey{BlockKey: block1Key, CodeIndex: 0}
	code2Key := CodeKey{BlockKey: block1Key, CodeIndex: 1}
	code3Key := CodeKey{BlockKey: block2Key, CodeIndex: 0}
	orchestrationKey := OrchestrationKey{ConfigKey: config3Key}
	phase1Key := PhaseKey{OrchestrationKey: orchestrationKey, PhaseIndex: 0}
	phase2Key := PhaseKey{OrchestrationKey: orchestrationKey, PhaseIndex: 1}
	phase3Key := PhaseKey{OrchestrationKey: orchestrationKey, PhaseIndex: 2}
	task1Key := TaskKey{PhaseKey: phase1Key, TaskIndex: 0}
	task2Key := TaskKey{PhaseKey: phase1Key, TaskIndex: 1}
	task3Key := TaskKey{PhaseKey: phase2Key, TaskIndex: 0}
	sharedCodeRow1Key := ConfigRowKey{ConfigKey: configSharedCodesKey, ConfigRowId: "11"}
	sharedCodeRow2Key := ConfigRowKey{ConfigKey: configSharedCodesKey, ConfigRowId: "12"}
	sharedCode1Key := SharedCodeKey{ConfigRowKey: sharedCodeRow1Key}
	sharedCode2Key := SharedCodeKey{ConfigRowKey: sharedCodeRow2Key}

	// Keys in sorted order
	keysSorted := []Key{
		branch1Key,
		branch2Key,
		branch3Key,
		config1Key,
		config2Key,
		config3Key,
		configSharedCodesKey,
		configRow1Key,
		configRow2Key,
		configRow3Key,
		sharedCodeRow1Key,
		sharedCodeRow2Key,
		sharedCode1Key,
		sharedCode2Key,
		transformationKey,
		orchestrationKey,
		block1Key,
		block2Key,
		block3Key,
		phase1Key,
		phase2Key,
		phase3Key,
		code1Key,
		code2Key,
		code3Key,
		task1Key,
		task2Key,
		task3Key,
	}

	// Keys in random order
	keys := make([]Key, len(keysSorted))
	copy(keys, keysSorted)
	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	assert.NotEqual(t, keysSorted, keys)

	// Test sort
	sorter := state.NewIdSorter()
	sort.SliceStable(keys, func(i, j int) bool {
		return sorter.Less(keys[i], keys[j])
	})
	assert.Equal(t, keysSorted, keys)

	// Check key logic path
	var logicPaths []string
	for _, key := range keys {
		logicPaths = append(logicPaths, key.LogicPath())
	}
	assert.Equal(t, []string{
		"branch:1",
		"branch:2",
		"branch:3",
		"branch:1/component:foo.bar/config:4",
		"branch:2/component:foo.bar/config:5",
		"branch:3/component:foo.bar/config:6",
		"branch:3/component:keboola.shared-code/config:7",
		"branch:1/component:foo.bar/config:4/row:8",
		"branch:1/component:foo.bar/config:4/row:9",
		"branch:1/component:foo.bar/config:4/row:10",
		"branch:3/component:keboola.shared-code/config:7/row:11",
		"branch:3/component:keboola.shared-code/config:7/row:12",
		"branch:3/component:keboola.shared-code/config:7/row:11/sharedCode",
		"branch:3/component:keboola.shared-code/config:7/row:12/sharedCode",
		"branch:2/component:foo.bar/config:5/transformation",
		"branch:3/component:foo.bar/config:6/orchestration",
		"branch:2/component:foo.bar/config:5/transformation/block:001",
		"branch:2/component:foo.bar/config:5/transformation/block:002",
		"branch:2/component:foo.bar/config:5/transformation/block:003",
		"branch:3/component:foo.bar/config:6/orchestration/phase:001",
		"branch:3/component:foo.bar/config:6/orchestration/phase:002",
		"branch:3/component:foo.bar/config:6/orchestration/phase:003",
		"branch:2/component:foo.bar/config:5/transformation/block:001/code:001",
		"branch:2/component:foo.bar/config:5/transformation/block:001/code:002",
		"branch:2/component:foo.bar/config:5/transformation/block:002/code:001",
		"branch:3/component:foo.bar/config:6/orchestration/phase:001/task:001",
		"branch:3/component:foo.bar/config:6/orchestration/phase:001/task:002",
		"branch:3/component:foo.bar/config:6/orchestration/phase:002/task:001",
	}, logicPaths)

	// Check key to string
	var strings []string
	for _, key := range keys {
		strings = append(strings, key.String())
	}
	assert.Equal(t, []string{
		`branch "branch:1"`,
		`branch "branch:2"`,
		`branch "branch:3"`,
		`config "branch:1/component:foo.bar/config:4"`,
		`config "branch:2/component:foo.bar/config:5"`,
		`config "branch:3/component:foo.bar/config:6"`,
		`config "branch:3/component:keboola.shared-code/config:7"`,
		`config row "branch:1/component:foo.bar/config:4/row:8"`,
		`config row "branch:1/component:foo.bar/config:4/row:9"`,
		`config row "branch:1/component:foo.bar/config:4/row:10"`,
		`config row "branch:3/component:keboola.shared-code/config:7/row:11"`,
		`config row "branch:3/component:keboola.shared-code/config:7/row:12"`,
		`shared code "branch:3/component:keboola.shared-code/config:7/row:11/sharedCode"`,
		`shared code "branch:3/component:keboola.shared-code/config:7/row:12/sharedCode"`,
		`transformation "branch:2/component:foo.bar/config:5/transformation"`,
		`orchestration "branch:3/component:foo.bar/config:6/orchestration"`,
		`block "branch:2/component:foo.bar/config:5/transformation/block:001"`,
		`block "branch:2/component:foo.bar/config:5/transformation/block:002"`,
		`block "branch:2/component:foo.bar/config:5/transformation/block:003"`,
		`phase "branch:3/component:foo.bar/config:6/orchestration/phase:001"`,
		`phase "branch:3/component:foo.bar/config:6/orchestration/phase:002"`,
		`phase "branch:3/component:foo.bar/config:6/orchestration/phase:003"`,
		`code "branch:2/component:foo.bar/config:5/transformation/block:001/code:001"`,
		`code "branch:2/component:foo.bar/config:5/transformation/block:001/code:002"`,
		`code "branch:2/component:foo.bar/config:5/transformation/block:002/code:001"`,
		`task "branch:3/component:foo.bar/config:6/orchestration/phase:001/task:001"`,
		`task "branch:3/component:foo.bar/config:6/orchestration/phase:001/task:002"`,
		`task "branch:3/component:foo.bar/config:6/orchestration/phase:002/task:001"`,
	}, strings)
}
