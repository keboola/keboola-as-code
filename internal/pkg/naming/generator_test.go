package naming

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestUniquePathSameObjectType(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = "{branch_name}"
	g.template.Config = "{component_type}/{component_id}/{config_name}"
	g.template.ConfigRow = "rows/{config_row_name}"
	component := &keboola.Component{ComponentKey: keboola.ComponentKey{ID: "foo"}, Type: "writer"}

	// Default branch
	assert.Equal(t, "main", g.BranchPath(&Branch{BranchKey: BranchKey{ID: 12}, Name: "a", IsDefault: true}).Path())
	assert.Equal(t, "main-001", g.BranchPath(&Branch{BranchKey: BranchKey{ID: 23}, Name: "b", IsDefault: true}).Path())

	// Branch
	assert.Equal(t, "branch-name", g.BranchPath(&Branch{BranchKey: BranchKey{ID: 56}, Name: "branchName"}).Path())
	assert.Equal(t, "branch-name-001", g.BranchPath(&Branch{BranchKey: BranchKey{ID: 78}, Name: "branch-name"}).Path())

	// Config
	assert.Equal(t, "my-branch/writer/foo/my-config", g.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{ID: "123"}, Name: "myConfig"}).Path())
	assert.Equal(t, "my-branch/writer/foo/my-config-001", g.ConfigPath("my-branch", component, &Config{ConfigKey: ConfigKey{ID: "234"}, Name: "my-config"}).Path())

	// Config row
	assert.Equal(t, "my-branch/config/rows/my-row", g.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "456"}, Name: "myRow"}).Path())
	assert.Equal(t, "my-branch/config/rows/my-row-001", g.ConfigRowPath("my-branch/config", component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "678"}, Name: "myRow"}).Path())
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = "prefix"
	g.template.Config = "prefix"
	g.template.ConfigRow = "prefix"
	component := &keboola.Component{ComponentKey: keboola.ComponentKey{ID: "foo"}, Type: "writer"}
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/prefix", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{ID: "123"}, Name: "a"}).Path())
	assert.Equal(t, "foo/prefix-001", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{ID: "234"}, Name: "b"}).Path())
	assert.Equal(t, "foo/prefix-002", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "345"}, Name: "c"}).Path())
	assert.Equal(t, "foo/prefix-003", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "456"}, Name: "d"}).Path())
	assert.Equal(t, "foo/prefix-004", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "567"}, Name: "", Content: rowWithName}).Path())
	assert.Equal(t, "foo/prefix-005", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "678"}, Name: "", Content: rowWithoutName}).Path())
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Parallel()
	g := NewGenerator(TemplateWithIds(), NewRegistry())
	g.template.Branch = ""
	g.template.Config = ""
	g.template.ConfigRow = ""
	component := &keboola.Component{ComponentKey: keboola.ComponentKey{ID: "foo"}, Type: "writer"}
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})
	parentPath := "foo"

	assert.Equal(t, "foo/config", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{ID: "123"}, Name: "a"}).Path())
	assert.Equal(t, "foo/config-001", g.ConfigPath(parentPath, component, &Config{ConfigKey: ConfigKey{ID: "234"}, Name: "b"}).Path())
	assert.Equal(t, "foo/config-row", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "345"}, Name: "c"}).Path())
	assert.Equal(t, "foo/config-row-001", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "456"}, Name: "d"}).Path())
	assert.Equal(t, "foo/config-row-002", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "567"}, Name: "", Content: rowWithName}).Path())
	assert.Equal(t, "foo/config-row-003", g.ConfigRowPath(parentPath, component, &ConfigRow{ConfigRowKey: ConfigRowKey{ID: "678"}, Name: "", Content: rowWithoutName}).Path())
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
					ID: 1234,
				},
				Name:      "my Super-BRANCH",
				IsDefault: false,
			},
		).Path())

	// Config
	assert.Equal(
		t,
		"my-branch/extractor/keboola.ex-foo-bar/456-my-production-config",
		g.ConfigPath(
			"my-branch",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{
					ID: "keboola.ex-foo-bar",
				},
				Type: "extractor",
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchID:    1234,
					ComponentID: "keboola.ex-foo-bar",
					ID:          "456",
				},
				Name: "MyProductionConfig",
			},
		).Path())

	// Config Row
	assert.Equal(
		t,
		"my-branch/my-row/rows/789-row-ab-c",
		g.ConfigRowPath(
			"my-branch/my-row",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: "keboola.ex-foo-bar"},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchID:    1234,
					ComponentID: "keboola.ex-foo-bar",
					ConfigID:    "456",
					ID:          "789",
				},
				Name: "---  row AbC---",
			},
		).Path())

	// Shared code (config)
	assert.Equal(
		t,
		"my-branch/_shared/keboola.python-transformation-v2",
		g.ConfigPath(
			"my-branch",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{
					ID: keboola.SharedCodeComponentID,
				},
				Type: "other",
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchID:    1234,
					ComponentID: keboola.SharedCodeComponentID,
					ID:          "456",
				},
				Name:    "MySharedCode",
				Content: orderedmap.New(),
				SharedCode: &SharedCodeConfig{
					Target: `keboola.python-transformation-v2`,
				},
			},
		).Path())

	// Scheduler
	assert.Equal(
		t,
		"my-branch/my-config/schedules/456-schedule-1",
		g.ConfigPath(
			"my-branch/my-config",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: keboola.SchedulerComponentID},
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchID:    1234,
					ComponentID: keboola.SchedulerComponentID,
					ID:          "456",
				},
				Relations: Relations{
					&SchedulerForRelation{
						ComponentID: `foo.bar`,
						ConfigID:    `789`,
					},
				},
				Name:    "schedule-1",
				Content: orderedmap.New(),
			},
		).Path())

	// Shared code (config row)
	assert.Equal(
		t,
		"my-branch/shared/codes/789-code-ab-c",
		g.ConfigRowPath(
			"my-branch/shared",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: keboola.SharedCodeComponentID},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchID:    1234,
					ComponentID: keboola.SharedCodeComponentID,
					ConfigID:    "456",
					ID:          "789",
				},
				Name: "---  code AbC---",
			},
		).Path())

	// VariablesConfig
	assert.Equal(
		t,
		"my-branch/my-config/variables",
		g.ConfigPath(
			"my-branch/my-config",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: keboola.VariablesComponentID},
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchID:    1234,
					ComponentID: keboola.VariablesComponentID,
					ID:          "456",
				},
				Relations: Relations{
					&VariablesForRelation{
						ConfigID:    `4567`,
						ComponentID: `foo.bar`,
					},
				},
				Name:    "Variables",
				Content: orderedmap.New(),
			},
		).Path())

	// DataAppConfig values
	assert.Equal(
		t,
		"my-branch/app/keboola.data-apps/456-some-data-app",
		g.ConfigPath(
			"my-branch",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: keboola.DataAppsComponentID},
			},
			&Config{
				ConfigKey: ConfigKey{
					BranchID:    1234,
					ComponentID: keboola.DataAppsComponentID,
					ID:          "456",
				},
				Name: "Some Data App",
			},
		).Path())

	// VariablesConfig values
	assert.Equal(
		t,
		"my-branch/my-config/variables/values/789-default-values",
		g.ConfigRowPath(
			"my-branch/my-config/variables",
			&keboola.Component{
				ComponentKey: keboola.ComponentKey{ID: keboola.VariablesComponentID},
			},
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchID:    1234,
					ComponentID: keboola.VariablesComponentID,
					ConfigID:    "456",
					ID:          "789",
				},
				Name: "Default Values",
			},
		).Path())
}
