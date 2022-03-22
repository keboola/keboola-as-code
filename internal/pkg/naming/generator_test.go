package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestUniquePathSameObjectType(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = "{branch_name}"
	g.template.Config = "{component_type}/{component_id}/{config_name}"
	g.template.ConfigRow = "rows/{config_row_name}"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}

	// Default branch
	assert.Equal(t, "main", g.BranchPath(&Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}).String())
	assert.Equal(t, "main-001", g.BranchPath(&Branch{BranchKey: BranchKey{Id: 23}, Name: "b", IsDefault: true}).String())

	// Branch
	assert.Equal(t, "branch-name", g.BranchPath(&Branch{BranchKey: BranchKey{Id: 56}, Name: "branchName"}).String())
	assert.Equal(t, "branch-name-001", g.BranchPath(&Branch{BranchKey: BranchKey{Id: 78}, Name: "branch-name"}).String())

	// Config
	assert.Equal(t, "my-branch/writer/foo/my-config", g.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "myConfig"}).String())
	assert.Equal(t, "my-branch/writer/foo/my-config-001", g.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "my-config"}).String())

	// Config row
	assert.Equal(t, "my-branch/config/rows/my-row", g.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "myRow"}).String())
	assert.Equal(t, "my-branch/config/rows/my-row-001", g.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "myRow"}).String())
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = "prefix"
	g.template.Config = "prefix"
	g.template.ConfigRow = "prefix"
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/prefix", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).String())
	assert.Equal(t, "foo/prefix-001", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).String())
	assert.Equal(t, "foo/prefix-002", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).String())
	assert.Equal(t, "foo/prefix-003", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).String())
	assert.Equal(t, "foo/prefix-004", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).String())
	assert.Equal(t, "foo/prefix-005", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).String())
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = ""
	g.template.Config = ""
	g.template.ConfigRow = ""
	component := &Component{ComponentKey: ComponentKey{Id: "foo"}, Type: "writer"}
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/config", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "123"}, Name: "a"}).String())
	assert.Equal(t, "foo/config-001", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{Id: "234"}, Name: "b"}).String())
	assert.Equal(t, "foo/config-row", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "345"}, Name: "c"}).String())
	assert.Equal(t, "foo/config-row-001", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "456"}, Name: "d"}).String())
	assert.Equal(t, "foo/config-row-002", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "567"}, Name: "", Content: rowWithName}).String())
	assert.Equal(t, "foo/config-row-003", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{Id: "678"}, Name: "", Content: rowWithoutName}).String())
}

func TestNamingDefaultTemplate(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())

	// Branch
	assert.Equal(
		t,
		"1234-my-super-branch",
		g.BranchPath(
			&Branch{
				BranchKey: BranchKey{
					Id: 1234,
				},
				Name:      "my Super-BRANCH",
				IsDefault: false,
			},
		).String())

	// Config
	assert.Equal(
		t,
		"my-branch/extractor/keboola.ex-foo-bar/456-my-production-config",
		g.ConfigPath(
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
		).String())

	// Config Row
	assert.Equal(
		t,
		"my-branch/my-row/rows/789-row-ab-c",
		g.ConfigRowPath(
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
		).String())

	// Shared code (config)
	assert.Equal(
		t,
		"my-branch/_shared/keboola.python-transformation-v2",
		g.ConfigPath(
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
				Name:    "MySharedCode",
				Content: orderedmap.New(),
				SharedCode: &SharedCodeConfig{
					Target: `keboola.python-transformation-v2`,
				},
			},
		).String())

	// Scheduler
	assert.Equal(
		t,
		"my-branch/my-config/schedules/456-schedule-1",
		g.ConfigPath(
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
				Content: orderedmap.New(),
			},
		).String())

	// Shared code (config row)
	assert.Equal(
		t,
		"my-branch/shared/codes/789-code-ab-c",
		g.ConfigRowPath(
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
		).String())

	// VariablesConfig
	assert.Equal(
		t,
		"my-branch/my-config/variables",
		g.ConfigPath(
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
				Content: orderedmap.New(),
			},
		).String())

	// VariablesConfig values
	assert.Equal(
		t,
		"my-branch/my-config/variables/values/789-default-values",
		g.ConfigRowPath(
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
		).String())
}
