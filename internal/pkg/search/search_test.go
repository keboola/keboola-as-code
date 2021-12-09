package search

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestStateSearchForBranches(t *testing.T) {
	t.Parallel()
	all := testBranches()
	assert.Len(t, Branches(all, `baz`), 0)
	assert.Len(t, Branches(all, `Foo bar`), 1)
	assert.Len(t, Branches(all, `a`), 2)
}

func TestStateSearchForBranch(t *testing.T) {
	t.Parallel()
	all := testBranches()

	b, err := Branch(all, `baz`)
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `no branch matches the specified "baz"`, err.Error())

	b, err = Branch(all, `Foo bar`)
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, "Foo Bar Branch", b.ObjectName())

	b, err = Branch(all, `a`)
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `multiple branches match the specified "a"`, err.Error())
}

func TestStateSearchForConfigs(t *testing.T) {
	t.Parallel()
	all := testConfigs()

	assert.Len(t, Configs(all, `baz`), 0)
	assert.Len(t, Configs(all, `1`), 1)
	assert.Len(t, Configs(all, `Config`), 2)
}

func TestStateSearchForConfig(t *testing.T) {
	t.Parallel()
	all := testConfigs()

	c, err := Config(all, `baz`)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `no config matches the specified "baz"`, err.Error())

	c, err = Config(all, `1`)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "Config 1", c.ObjectName())

	c, err = Config(all, `config`)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `multiple configs match the specified "config"`, err.Error())
}

func TestStateSearchForConfigRows(t *testing.T) {
	t.Parallel()
	all := testRows()

	assert.Len(t, ConfigRows(all, `baz`), 0)
	assert.Len(t, ConfigRows(all, `1`), 1)
	assert.Len(t, ConfigRows(all, `row`), 2)
}

func TestStateSearchForConfigRow(t *testing.T) {
	t.Parallel()
	all := testRows()

	r, err := ConfigRow(all, `baz`)
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `no row matches the specified "baz"`, err.Error())

	r, err = ConfigRow(all, `1`)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, "Config Row 1", r.ObjectName())

	r, err = ConfigRow(all, `row`)
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `multiple rows match the specified "row"`, err.Error())
}

func TestMatchObjectIdOrName(t *testing.T) {
	t.Parallel()

	// Match by ID
	assert.True(t, matchObjectIdOrName(`123`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIdOrName(`1234`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIdOrName(`12`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc",
	}))

	// Match by name
	assert.True(t, matchObjectIdOrName(`abc`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIdOrName(`def`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIdOrName(`abc def`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.False(t, matchObjectIdOrName(`foo`, &model.Branch{
		BranchKey: model.BranchKey{Id: 123},
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

func testConfigs() []*model.Config {
	return []*model.Config{
		{
			Name: "Config 1",
		},
		{
			Name: "Config 2",
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
