package naming

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
)

type testCase struct {
	expected string
	object   Object
}

func TestUniquePathSameObjectType(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = "{branch_name}"
	g.template.Config = "{component_type}/{component_id}/{config_name}"
	g.template.ConfigRow = "rows/{config_row_name}"

	assertCases(t, g, []testCase{
		// Default branch
		{"main", &Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}},
		{"main", &Branch{BranchKey: BranchKey{Id: 12}, Name: "a", IsDefault: true}},
		{"main-001", &Branch{BranchKey: BranchKey{Id: 23}, Name: "b", IsDefault: true}},
		// Branch
		{"branch-name", &Branch{BranchKey: BranchKey{Id: 56}, Name: "branchName"}},
		{"branch-name-001", &Branch{BranchKey: BranchKey{Id: 78}, Name: "branch-name"}},
		// Config
		{
			"my-branch/writer/keboola.wr-foo-bar/my-config",
			&Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "123"}, Name: "myConfig"},
		},
		{
			"my-branch/writer/keboola.wr-foo-bar/my-config-001",
			&Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "234"}, Name: "my-config"},
		},
		// Config row
		{
			"my-branch/my-writer/rows/my-row",
			&ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "456"}, Name: "myRow"},
		},
		{
			"my-branch/my-writer/rows/my-row-001",
			&ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "678"}, Name: "myRow"},
		},
	})
}

func TestUniquePathDifferentObjects(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = "prefix"
	g.template.Config = "prefix"
	g.template.ConfigRow = "prefix"
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})

	assertCases(t, g, []testCase{
		{"my-branch/prefix", &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "123"}, Name: "a"}},
		{"my-branch/prefix-001", &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.foo-bar", Id: "234"}, Name: "b"}},
		{"my-branch/my-config/prefix", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "345"}, Name: "c"}},
		{"my-branch/my-config/prefix-001", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "456"}, Name: "d"}},
		{"my-branch/my-config/prefix-002", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "567"}, Name: "", Content: rowWithName}},
		{"my-branch/my-config/prefix-003", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.foo-bar", ConfigId: "1", Id: "678"}, Name: "", Content: rowWithoutName}},
	})
}

func TestNamingEmptyTemplate(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)
	g.template.Branch = ""
	g.template.Config = ""
	g.template.ConfigRow = ""
	rowWithName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "name", Value: "my-name"}})
	rowWithoutName := orderedmap.FromPairs([]orderedmap.Pair{{Key: "foo", Value: "bar"}})

	assertCases(t, g, []testCase{
		{"my-branch/config", &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "123"}, Name: "a"}},
		{"my-branch/config-001", &Config{ConfigKey: ConfigKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", Id: "234"}, Name: "b"}},
		{"my-branch/my-writer/config-row", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "345"}, Name: "c"}},
		{"my-branch/my-writer/config-row-001", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "456"}, Name: "d"}},
		{"my-branch/my-writer/config-row-002", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "567"}, Name: "", Content: rowWithName}},
		{"my-branch/my-writer/config-row-003", &ConfigRow{ConfigRowKey: ConfigRowKey{BranchId: 1, ComponentId: "keboola.wr-foo-bar", ConfigId: "1", Id: "678"}, Name: "", Content: rowWithoutName}},
	})
}

func TestNamingDefaultTemplate(t *testing.T) {
	t.Parallel()
	g := testGenerator(t)

	assertCases(t, g, []testCase{
		// Branch
		{
			"1234-my-super-branch",
			&Branch{
				BranchKey: BranchKey{
					Id: 1234,
				},
				Name:      "my Super-BRANCH",
				IsDefault: false,
			},
		},
		// Config
		{
			"my-branch/extractor/keboola.ex-foo-bar/456-my-production-config",
			&Config{
				ConfigKey: ConfigKey{
					BranchId:    1,
					ComponentId: "keboola.ex-foo-bar",
					Id:          "456",
				},
				Name: "MyProductionConfig",
			},
		},
		// Config Row
		{
			"my-branch/my-extractor/rows/789-row-ab-c",
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1,
					ComponentId: "keboola.ex-foo-bar",
					ConfigId:    "1",
					Id:          "789",
				},
				Name: "---  row AbC---",
			},
		},
		// Shared code (config)
		{
			"my-branch/_shared/keboola.python-transformation-v2",
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
		},
		// Scheduler
		{
			"my-branch/my-config/schedules/456-schedule-1",
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
		},
		// Shared code (config row)
		{
			"my-branch/_shared/keboola.python-transformation-v2",
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
		},
		// VariablesConfig
		{
			"my-branch/my-config/variables",
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
		},
		// Variables values
		{
			"my-branch/my-config/my-variables/values/789-default-values",
			&ConfigRow{
				ConfigRowKey: ConfigRowKey{
					BranchId:    1,
					ComponentId: VariablesComponentId,
					ConfigId:    "1",
					Id:          "789",
				},
				Name: "Default Values",
			},
		},
	})
}

func assertCases(t *testing.T, g *Generator, cases []testCase) {
	for i, c := range cases {
		assert.Equal(t, c.expected, generatePath(t, g, c.object), fmt.Sprintf(`case "%d"`, i))
	}
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
	objects := state.NewCollection(state.NewIdSorter())
	return NewGenerator(TemplateWithIds(), registry, NewComponentsMap(testapi.NewMockedComponentsProvider()), objects)
}

func generatePath(t *testing.T, g *Generator, object Object) string {
	t.Helper()
	path, err := g.Generate(object)
	if err != nil {
		t.Fatal(err)
	}
	return path.String()
}
