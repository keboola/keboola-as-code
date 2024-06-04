package search

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestStateSearchForBranches(t *testing.T) {
	t.Parallel()
	all := testBranches()
	assert.Empty(t, Branches(all, `baz`))
	assert.Len(t, Branches(all, `Foo bar`), 1)
	assert.Len(t, Branches(all, `a`), 2)
}

func TestStateSearchForBranch(t *testing.T) {
	t.Parallel()
	all := testBranches()

	b, err := Branch(all, `baz`)
	require.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `no branch matches the specified "baz"`, err.Error())

	b, err = Branch(all, `Foo bar`)
	require.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, "Foo Bar Branch", b.ObjectName())

	b, err = Branch(all, `a`)
	require.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `multiple branches match the specified "a"`, err.Error())
}

func TestStateSearchForConfigs(t *testing.T) {
	t.Parallel()
	all := testConfigs()

	assert.Empty(t, Configs(all, `baz`))
	assert.Len(t, Configs(all, `1`), 1)
	assert.Len(t, Configs(all, `Config`), 2)
}

func TestStateSearchForConfig(t *testing.T) {
	t.Parallel()
	all := testConfigs()

	c, err := Config(all, `baz`)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `no config matches the specified "baz"`, err.Error())

	c, err = Config(all, `1`)
	require.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "Config 1", c.ObjectName())

	c, err = Config(all, `config`)
	require.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `multiple configs match the specified "config"`, err.Error())
}

func TestStateSearchForConfigRows(t *testing.T) {
	t.Parallel()
	all := testRows()

	assert.Empty(t, ConfigRows(all, `baz`))
	assert.Len(t, ConfigRows(all, `1`), 1)
	assert.Len(t, ConfigRows(all, `row`), 2)
}

func TestStateSearchForConfigRow(t *testing.T) {
	t.Parallel()
	all := testRows()

	r, err := ConfigRow(all, `baz`)
	require.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `no row matches the specified "baz"`, err.Error())

	r, err = ConfigRow(all, `1`)
	require.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, "Config Row 1", r.ObjectName())

	r, err = ConfigRow(all, `row`)
	require.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `multiple rows match the specified "row"`, err.Error())
}

func TestStateSearchForConfigsInTemplate(t *testing.T) {
	t.Parallel()
	all := []*model.ConfigWithRows{
		{
			Config: &model.Config{Name: "Config 1", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst1"}},
		},
		{
			Config: &model.Config{Name: "Config 2"},
		},
		{
			Config: &model.Config{Name: "Config 3", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst2"}},
		},
	}

	res := ConfigsForTemplateInstance(all, `inst1`)
	assert.Len(t, res, 1)
	assert.Equal(t, "Config 1", res[0].Name)
}

func TestConfigsByTemplateInstance(t *testing.T) {
	t.Parallel()

	all := []*model.ConfigWithRows{
		{
			Config: &model.Config{Name: "Config 1", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst1"}},
		},
		{
			Config: &model.Config{Name: "Config 2", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst1"}},
		},
		{
			Config: &model.Config{Name: "Config 3"},
		},
		{
			Config: &model.Config{Name: "Config 4", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst2"}},
		},
	}

	res := ConfigsByTemplateInstance(all)
	assert.Equal(t, map[string][]*model.ConfigWithRows{
		"inst1": {
			{
				Config: &model.Config{Name: "Config 1", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst1"}},
			},
			{
				Config: &model.Config{Name: "Config 2", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst1"}},
			},
		},
		"inst2": {
			{
				Config: &model.Config{Name: "Config 4", Metadata: model.ConfigMetadata{"KBC.KAC.templates.instanceId": "inst2"}},
			},
		},
	}, res)
}

func TestMatchObjectIdOrName(t *testing.T) {
	t.Parallel()

	// Match by ID
	assert.True(t, matchObjectIDOrName(`123`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIDOrName(`1234`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIDOrName(`12`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc",
	}))

	// Match by name
	assert.True(t, matchObjectIDOrName(`abc`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIDOrName(`def`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIDOrName(`abc def`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc Def",
	}))
	assert.False(t, matchObjectIDOrName(`foo`, &model.Branch{
		BranchKey: model.BranchKey{ID: 123},
		Name:      "Abc Def",
	}))
}

func testBranches() []*model.Branch {
	return []*model.Branch{
		{
			Name:      "Main",
			IsDefault: true,
		},
		{
			Name:      "Foo Bar Branch",
			IsDefault: false,
		},
	}
}

func testConfigs() []*model.ConfigWithRows {
	return []*model.ConfigWithRows{
		{
			Config: &model.Config{Name: "Config 1"},
		},
		{
			Config: &model.Config{Name: "Config 2"},
		},
	}
}

func testRows() []*model.ConfigRow {
	return []*model.ConfigRow{
		{
			Name: "Config Row 1",
		},
		{
			Name: "Config Row 2",
		},
	}
}
