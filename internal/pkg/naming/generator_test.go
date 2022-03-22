package naming

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

func TestUniquePathSameObjectType(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = "{branch_name}"
	g.template.Config = "{component_type}/{component_id}/{config_name}"
	g.template.ConfigRow = "rows/{config_row_name}"

	// Default branch
	assert.Equal(t, "main", objectPath(t, g, &Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}))
	assert.Equal(t, "main-001", objectPath(t, g, &Branch{BranchKey: BranchKey{Id: 23}, Name: "b", IsDefault: true}))

	// Branch
	assert.Equal(t, "branch-name", objectPath(t, g, &Branch{BranchKey: BranchKey{Id: 56}, Name: "branchName"}))
	assert.Equal(t, "branch-name-001", objectPath(t, g, &Branch{BranchKey: BranchKey{Id: 78}, Name: "branch-name"}))

	// Config
	assert.Equal(t, "my-branch/writer/keboola.wr-foo-bar/my-config", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "123"}, Name: "myConfig"}))
	assert.Equal(t, "my-branch/writer/keboola.wr-foo-bar/my-config-001", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "234"}, Name: "my-config"}))

	// Config row
	assert.Equal(t, "my-branch/my-writer/rows/my-row", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "456"}, Name: "myRow"}))
	assert.Equal(t, "my-branch/my-writer/rows/my-row-001", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "678"}, Name: "myRow"}))
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = "prefix"
	g.template.Config = "prefix"
	g.template.ConfigRow = "prefix"
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})

	assert.Equal(t, "my-branch/prefix", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "123"}, Name: "a"}))
	assert.Equal(t, "my-branch/prefix-001", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "234"}, Name: "b"}))
	assert.Equal(t, "my-branch/my-config/prefix", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "345"}, Name: "c"}))
	assert.Equal(t, "my-branch/my-config/prefix-001", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "456"}, Name: "d"}))
	assert.Equal(t, "my-branch/my-config/prefix-002", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "567"}, Name: "", Content: rowWithName}))
	assert.Equal(t, "my-branch/my-config/prefix-003", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "678"}, Name: "", Content: rowWithoutName}))
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = ""
	g.template.Config = ""
	g.template.ConfigRow = ""
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})

	assert.Equal(t, "my-branch/config", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "123"}, Name: "a"}))
	assert.Equal(t, "my-branch/config-001", objectPath(t, g, &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "234"}, Name: "b"}))
	assert.Equal(t, "my-branch/my-writer/config-row", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "345"}, Name: "c"}))
	assert.Equal(t, "my-branch/my-writer/config-row-001", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "456"}, Name: "d"}))
	assert.Equal(t, "my-branch/my-writer/config-row-002", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "567"}, Name: "", Content: rowWithName}))
	assert.Equal(t, "my-branch/my-writer/config-row-003", objectPath(t, g, &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "678"}, Name: "", Content: rowWithoutName}))
}

func TestNamingDefaultTemplate(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)

	// Branch
	assert.Equal(
		t,
		"1234-my-super-branch",
		objectPath(t, g,
			&Branch{
				BranchKey: BranchKey{
					Id: 1234,
				},
				Name:      "my Super-BRANCH",
				IsDefault: false,
			},
		))

	// Config
	assert.Equal(
		t,
		"my-branch/extractor/keboola.ex-foo-bar/456-my-production-config",
		objectPath(t, g,
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1,
					ComponentId: "keboola.ex-foo-bar",
					Id:          "456",
				},
				Name: "MyProductionConfig",
			},
		))

	// Config Row
	assert.Equal(
		t,
		"my-branch/my-extractor/rows/789-row-ab-c",
		objectPath(t, g,
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1,
					ComponentId: "keboola.ex-foo-bar",
					ConfigId:    "1",
					Id:          "789",
				},
				Name: "---  row AbC---",
			},
		))

	// Shared code (config)
	assert.Equal(
		t,
		"my-branch/_shared/keboola.python-transformation-v2",
		objectPath(t, g,
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1,
					ComponentId: SharedCodeComponentId,
					Id:          "456",
				},
				Name:    "MySharedCode",
				Content: orderedmap.New(),
				SharedCode: &SharedCodeConfig{
					Target: `keboola.python-transformation-v2`,
				},
			},
		))

	// Scheduler
	assert.Equal(
		t,
		"my-branch/my-config/schedules/456-schedule-1",
		objectPath(t, g,
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1,
					ComponentId: SchedulerComponentId,
					Id:          "456",
				},
				Relations: Relations{
					&SchedulerForRelation{
						ComponentId: `keboola.foo-bar`,
						ConfigId:    `1`,
					},
				},
				Name:    "schedule-1",
				Content: orderedmap.New(),
			},
		))

	// Shared code (config row)
	assert.Equal(
		t,
		"my-branch/my-shared-code/codes/789-code-ab-c",
		objectPath(t, g,
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1,
					ComponentId: SharedCodeComponentId,
					ConfigId:    "1",
					Id:          "789",
				},
				Name: "---  code AbC---",
			},
		))

	// VariablesConfig
	assert.Equal(
		t,
		"my-branch/my-config/variables",
		objectPath(t, g,
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1,
					ComponentId: VariablesComponentId,
					Id:          "456",
				},
				Relations: Relations{
					&VariablesForRelation{
						ConfigId:    `1`,
						ComponentId: `keboola.foo-bar`,
					},
				},
				Name:    "Variables",
				Content: orderedmap.New(),
			},
		))

	// VariablesConfig values
	assert.Equal(
		t,
		"my-branch/my-config/my-variables/values/789-default-values",
		objectPath(t, g,
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1,
					ComponentId: VariablesComponentId,
					ConfigId:    "1",
					Id:          "789",
				},
				Name: "Default Values",
			},
		))
}

func testGenerator(t *testing.T) *Generator {
	t.Helper()
	registry := NewRegistry()
	assert.NoError(t, registry.Attach(BranchKey{Id: 1}, NewAbsPath("", "my-branch")))
	assert.NoError(t, registry.Attach(ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "1"}, NewAbsPath("my-branch", "my-config")))
	assert.NoError(t, registry.Attach(ConfigKey{BranchId: 1, ComponentId: "keboola.ex-foo-bar", Id: "1"}, NewAbsPath("my-branch", "my-extractor")))
	assert.NoError(t, registry.Attach(ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "1"}, NewAbsPath("my-branch", "my-writer")))
	assert.NoError(t, registry.Attach(ConfigKey{BranchId: 1, ComponentId: SharedCodeComponentId, Id: "1"}, NewAbsPath("my-branch", "my-shared-code")))
	assert.NoError(t, registry.Attach(ConfigKey{BranchId: 1, ComponentId: VariablesComponentId, Id: "1"}, NewAbsPath("my-branch/my-config", "my-variables")))
	return NewGenerator(TemplateWithIds(), registry, NewComponentsMap(testapi.NewMockedComponentsProvider()))
}

func objectPath(t *testing.T, g *Generator, object WithKey) string {
	t.Helper()
	path, err := g.PathFor(object)
	if err != nil {
		t.Fatal(err)
	}
	return path.String()
}
