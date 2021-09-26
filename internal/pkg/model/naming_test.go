package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestDefaultNaming(t *testing.T) {
	n := DefaultNaming()

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
		).RelativePath())

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
		).RelativePath())

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
		).RelativePath())

	// Shared code (config)
	assert.Equal(
		t,
		"my-branch/_shared/keboola.python-transformation-v2",
		n.ConfigPath(
			"my-branch",
			&Component{
				ComponentKey: ComponentKey{
					Id: ShareCodeComponentId,
				},
				Type: "other",
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1234,
					ComponentId: ShareCodeComponentId,
					Id:          "456",
				},
				Name: "MySharedCode",
				Content: utils.PairsToOrderedMap([]utils.Pair{
					{Key: ShareCodeTargetComponentKey, Value: `keboola.python-transformation-v2`},
				}),
			},
		).RelativePath())

	// Shared code (config row)
	assert.Equal(
		t,
		"my-branch/shared/codes/789-code-ab-c",
		n.ConfigRowPath(
			"my-branch/shared",
			&Component{
				ComponentKey: ComponentKey{Id: ShareCodeComponentId},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1234,
					ComponentId: ShareCodeComponentId,
					ConfigId:    "456",
					Id:          "789",
				},
				Name: "---  code AbC---",
			},
		).RelativePath())
}

func TestNamingAttachDetach(t *testing.T) {
	n := DefaultNaming()

	// Attach multiple times with same key
	key1 := BranchKey{Id: 123}
	n.Attach(key1, "my-branch")
	n.Attach(key1, "my-branch-123")
	n.Attach(key1, "my-branch-abc")
	assert.Len(t, n.usedByPath, 1)
	assert.Len(t, n.usedByKey, 1)
	assert.Equal(t, key1.String(), n.usedByPath["my-branch-abc"])
	assert.Equal(t, "my-branch-abc", n.usedByKey[key1.String()])

	// Attach another key
	key2 := BranchKey{Id: 456}
	n.Attach(key2, "my-branch-456")
	assert.Len(t, n.usedByPath, 2)
	assert.Len(t, n.usedByKey, 2)

	// Attach another key with same path
	msg := `naming error: path "my-branch-456" is attached to object "01_456_branch", but new object "01_789_branch" has same path`
	assert.PanicsWithError(t, msg, func() {
		n.Attach(BranchKey{Id: 789}, "my-branch-456")
	})

	// Detach
	n.Detach(key2)
	assert.Len(t, n.usedByPath, 1)
	assert.Len(t, n.usedByKey, 1)

	// Re-use path
	n.Attach(BranchKey{Id: 789}, "my-branch-456")
	assert.Len(t, n.usedByPath, 2)
	assert.Len(t, n.usedByKey, 2)
}

func TestUniquePathSameObjectType(t *testing.T) {
	t.Skipped()
	n := DefaultNaming()
	n.Branch = "{branch_name}"
	n.Config = "{component_type}/{component_id}/{config_name}"
	n.ConfigRow = "rows/{config_row_name}"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}

	// Default branch
	assert.Equal(t, "main", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}).RelativePath())
	assert.Equal(t, "main-001", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 23}, Name: "b", IsDefault: true}).RelativePath())

	// Branch
	assert.Equal(t, "branch-name", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 56}, Name: "branchName"}).RelativePath())
	assert.Equal(t, "branch-name-001", n.BranchPath(&Branch{BranchKey: BranchKey{Id: 78}, Name: "branch-name"}).RelativePath())

	// Config
	assert.Equal(t, "my-branch/writer/foo/my-config", n.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "myConfig"}).RelativePath())
	assert.Equal(t, "my-branch/writer/foo/my-config-001", n.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "my-config"}).RelativePath())

	// Config row
	assert.Equal(t, "my-branch/config/rows/my-row", n.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "myRow"}).RelativePath())
	assert.Equal(t, "my-branch/config/rows/my-row-001", n.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "myRow"}).RelativePath())
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Skipped()
	n := DefaultNaming()
	n.Branch = "prefix"
	n.Config = "prefix"
	n.ConfigRow = "prefix"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := utils.PairsToOrderedMap([]utils.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := utils.PairsToOrderedMap([]utils.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/prefix", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).RelativePath())
	assert.Equal(t, "foo/prefix-001", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).RelativePath())
	assert.Equal(t, "foo/prefix-002", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).RelativePath())
	assert.Equal(t, "foo/prefix-003", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).RelativePath())
	assert.Equal(t, "foo/prefix-004", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).RelativePath())
	assert.Equal(t, "foo/prefix-005", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).RelativePath())
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Skipped()
	n := DefaultNaming()
	n.Branch = ""
	n.Config = ""
	n.ConfigRow = ""
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := utils.PairsToOrderedMap([]utils.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := utils.PairsToOrderedMap([]utils.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/config", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).RelativePath())
	assert.Equal(t, "foo/config-001", n.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).RelativePath())
	assert.Equal(t, "foo/config-row", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).RelativePath())
	assert.Equal(t, "foo/config-row-001", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).RelativePath())
	assert.Equal(t, "foo/config-row-002", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).RelativePath())
	assert.Equal(t, "foo/config-row-003", n.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).RelativePath())
}
