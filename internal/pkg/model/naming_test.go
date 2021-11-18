package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestDefaultNaming(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()

	// Branch
	assert.Equal(
		t,
		"1234-my-super-branch",
		n.BranchPath(
			&Branch{
				BranchKey: BranchKey{
					Id: 1234,
				},
				Name:      "my Super-BRANCH",
				IsDefault: false,
			},
		).Path())

	// Config
	assert.Equal(
		t,
		"my-branch/extractor/keboola.ex-foo-bar/456-my-production-config",
		n.ConfigPath(
			"my-branch",
			&Component{
				ComponentKey: ComponentKey{
					Id: "keboola.ex-foo-bar",
				},
				Type: "extractor",
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1234,
					ComponentId: "keboola.ex-foo-bar",
					Id:          "456",
				},
				Name: "MyProductionConfig",
			},
		).Path())

	// Config Row
	assert.Equal(
		t,
		"my-branch/my-row/rows/789-row-ab-c",
		n.ConfigRowPath(
			"my-branch/my-row",
			&Component{
				ComponentKey: ComponentKey{Id: "keboola.ex-foo-bar"},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1234,
					ComponentId: "keboola.ex-foo-bar",
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "---  row AbC---",
			},
		).Path())

	// Shared code (config)
	assert.Equal(
		t,
		"my-branch/_shared/keboola.python-transformation-v2",
		n.ConfigPath(
			"my-branch",
			&Component{
				ComponentKey: ComponentKey{
					Id: SharedCodeComponentId,
				},
				Type: "other",
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1234,
					ComponentId: SharedCodeComponentId,
					Id:          "456",
				},
				Name: "MySharedCode",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{Key: ShareCodeTargetComponentKey, Value: `keboola.python-transformation-v2`},
				}),
			},
		).Path())

	// Scheduler
	assert.Equal(
		t,
		"my-branch/my-config/schedules/456-schedule-1",
		n.ConfigPath(
			"my-branch/my-config",
			&Component{
				ComponentKey: ComponentKey{Id: SchedulerComponentId},
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1234,
					ComponentId: SchedulerComponentId,
					Id:          "456",
				},
				Relations: Relations{
					&SchedulerForRelation{
						ComponentId: `foo.bar`,
						ConfigId:    `789`,
					},
				},
				Name:    "schedule-1",
				Content: utils.NewOrderedMap(),
			},
		).Path())

	// Shared code (config row)
	assert.Equal(
		t,
		"my-branch/shared/codes/789-code-ab-c",
		n.ConfigRowPath(
			"my-branch/shared",
			&Component{
				ComponentKey: ComponentKey{Id: SharedCodeComponentId},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1234,
					ComponentId: SharedCodeComponentId,
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "---  code AbC---",
			},
		).Path())

	// VariablesConfig
	assert.Equal(
		t,
		"my-branch/my-config/variables",
		n.ConfigPath(
			"my-branch/my-config",
			&Component{
				ComponentKey: ComponentKey{Id: VariablesComponentId},
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1234,
					ComponentId: VariablesComponentId,
					Id:          "456",
				},
				Relations: Relations{
					&VariablesForRelation{
						ConfigId:    `4567`,
						ComponentId: `foo.bar`,
					},
				},
				Name:    "Variables",
				Content: utils.NewOrderedMap(),
			},
		).Path())

	// VariablesConfig values
	assert.Equal(
		t,
		"my-branch/my-config/variables/values/default-values",
		n.ConfigRowPath(
			"my-branch/my-config/variables",
			&Component{
				ComponentKey: ComponentKey{Id: VariablesComponentId},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1234,
					ComponentId: VariablesComponentId,
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "Default Values",
			},
		).Path())
}

func TestNamingAttachDetach(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()

	// Attach multiple times with same key
	key1 := BranchKey{Id: 123}
	n.Attach(key1, NewPathInProject("", "my-branch"))
	n.Attach(key1, NewPathInProject("", "my-branch-123"))
	n.Attach(key1, NewPathInProject("", "my-branch-abc"))
	assert.Len(t, n.usedByPath, 1)
	assert.Len(t, n.usedByKey, 1)
	assert.Equal(t, key1, n.usedByPath["my-branch-abc"])
	assert.Equal(t, NewPathInProject("", "my-branch-abc"), n.usedByKey[key1.String()])

	// Attach another key
	key2 := BranchKey{Id: 456}
	n.Attach(key2, NewPathInProject("", "my-branch-456"))
	assert.Len(t, n.usedByPath, 2)
	assert.Len(t, n.usedByKey, 2)

	// Attach another key with same path
	msg := `naming error: path "my-branch-456" is attached to branch "456", but new branch "789" has same path`
	assert.PanicsWithError(t, msg, func() {
		n.Attach(BranchKey{Id: 789}, NewPathInProject("", "my-branch-456"))
	})

	// Detach
	n.Detach(key2)
	assert.Len(t, n.usedByPath, 1)
	assert.Len(t, n.usedByKey, 1)

	// Re-use path
	n.Attach(BranchKey{Id: 789}, NewPathInProject("", "my-branch-456"))
	assert.Len(t, n.usedByPath, 2)
	assert.Len(t, n.usedByKey, 2)
}

func TestUniquePathSameObjectType(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	n.Branch = "{branch_name}"
	n.Config = "{component_type}/{component_id}/{config_name}"
	n.ConfigRow = "rows/{config_row_name}"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}

	// Default branch
	assert.Equal(t, "main", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}).Path())
	assert.Equal(t, "main-001", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 23}, Name: "b", IsDefault: true}).Path())

	// Branch
	assert.Equal(t, "branch-name", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 56}, Name: "branchName"}).Path())
	assert.Equal(t, "branch-name-001", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 78}, Name: "branch-name"}).Path())

	// Config
	assert.Equal(t, "my-branch/writer/foo/my-config", n.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "myConfig"}).Path())
	assert.Equal(t, "my-branch/writer/foo/my-config-001", n.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "my-config"}).Path())

	// Config row
	assert.Equal(t, "my-branch/config/rows/my-row", n.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "myRow"}).Path())
	assert.Equal(t, "my-branch/config/rows/my-row-001", n.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "myRow"}).Path())
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	n.Branch = "prefix"
	n.Config = "prefix"
	n.ConfigRow = "prefix"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := utils.PairsToOrderedMap([]utils.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := utils.PairsToOrderedMap([]utils.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/prefix", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).Path())
	assert.Equal(t, "foo/prefix-001", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).Path())
	assert.Equal(t, "foo/prefix-002", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).Path())
	assert.Equal(t, "foo/prefix-003", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).Path())
	assert.Equal(t, "foo/prefix-004", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).Path())
	assert.Equal(t, "foo/prefix-005", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).Path())
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	n.Branch = ""
	n.Config = ""
	n.ConfigRow = ""
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := utils.PairsToOrderedMap([]utils.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := utils.PairsToOrderedMap([]utils.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/config", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).Path())
	assert.Equal(t, "foo/config-001", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).Path())
	assert.Equal(t, "foo/config-row", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).Path())
	assert.Equal(t, "foo/config-row-001", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).Path())
	assert.Equal(t, "foo/config-row-002", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).Path())
	assert.Equal(t, "foo/config-row-003", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).Path())
}

func TestNamingMatchConfigPathNotMatched(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	componentId, err := n.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"foo",
		))
	assert.NoError(t, err)
	assert.Empty(t, componentId)
}

func TestNamingMatchConfigPathOrdinary(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	componentId, err := n.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"extractor/keboola.ex-db-mysql/with-rows",
		))
	assert.NoError(t, err)
	assert.Equal(t, `keboola.ex-db-mysql`, componentId)
}

func TestNamingMatchConfigPathSharedCode(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	componentId, err := n.MatchConfigPath(
		BranchKey{},
		NewPathInProject(
			"parent/path",
			"_shared/keboola.python-transformation-v2",
		))
	assert.NoError(t, err)
	assert.Equal(t, SharedCodeComponentId, componentId)
}

func TestNamingMatchConfigPathVariables(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	componentId, err := n.MatchConfigPath(
		ConfigKey{},
		NewPathInProject(
			"parent/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, VariablesComponentId, componentId)
}

func TestNamingMatchSharedCodeVariables(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	componentId, err := n.MatchConfigPath(
		ConfigRowKey{ComponentId: SharedCodeComponentId},
		NewPathInProject(
			"shared/code/path",
			"variables",
		))
	assert.NoError(t, err)
	assert.Equal(t, VariablesComponentId, componentId)
}

func TestNamingMatchConfigRowPathNotMatched(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	matched := n.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: "foo.bar"},
		},
		NewPathInProject(
			"parent/path",
			"foo",
		),
	)
	assert.False(t, matched)
}

func TestNamingMatchConfigRowPathOrdinary(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	matched := n.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: "foo.bar"},
		},
		NewPathInProject(
			"parent/path",
			"rows/foo",
		),
	)
	assert.True(t, matched)
}

func TestNamingMatchConfigRowPathSharedCode(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	matched := n.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: SharedCodeComponentId},
		},
		NewPathInProject(
			"parent/path",
			"codes/foo",
		))
	assert.True(t, matched)
}

func TestNamingMatchConfigRowPathVariables(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	matched := n.MatchConfigRowPath(
		&Component{
			ComponentKey: ComponentKey{Id: VariablesComponentId},
		},
		NewPathInProject(
			"parent/path",
			"values/foo",
		))
	assert.True(t, matched)
}

func TestCodeFileExt(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	assert.Equal(t, `sql`, n.CodeFileExt(`keboola.snowflake-transformation`))
	assert.Equal(t, `py`, n.CodeFileExt(`keboola.python-transformation-v2`))
}

func TestCodeFileComment(t *testing.T) {
	t.Parallel()
	n := DefaultNamingWithIds()
	assert.Equal(t, `--`, n.CodeFileComment(`sql`))
	assert.Equal(t, `#`, n.CodeFileComment(`py`))
}
