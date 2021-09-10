package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/utils"
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
	assert.Equal(t, "my-branch/config/rows/my-row", n.ConfigRowPath("my-branch/config", &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "myRow"}).RelativePath())
	assert.Equal(t, "my-branch/config/rows/my-row-001", n.ConfigRowPath("my-branch/config", &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "myRow"}).RelativePath())
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
	assert.Equal(t, "foo/prefix-002", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).RelativePath())
	assert.Equal(t, "foo/prefix-003", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).RelativePath())
	assert.Equal(t, "foo/prefix-004", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).RelativePath())
	assert.Equal(t, "foo/prefix-005", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).RelativePath())
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
	assert.Equal(t, "foo/config-row", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).RelativePath())
	assert.Equal(t, "foo/config-row-001", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).RelativePath())
	assert.Equal(t, "foo/config-row-002", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).RelativePath())
	assert.Equal(t, "foo/config-row-003", n.ConfigRowPath(parentPath, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).RelativePath())
}
