package model

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestNewState(t *testing.T) {
	t.Parallel()
	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, `/`)
	assert.NoError(t, err)
	s := NewState(logger, fs, NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, s)
}

func TestStateComponents(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.NotNil(t, s.Components())
}

func TestStateAll(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.All(), 6)
}

func TestStateBranches(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.Branches(), 2)
}

func TestStateConfigs(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.Configs(), 2)
}

func TestStateConfigRows(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.ConfigRows(), 2)
}

func TestStateConfigsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 123}), 2)
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 567}), 0)
	assert.Len(t, s.ConfigsFrom(BranchKey{Id: 111}), 0)
}

func TestStateConfigRowsFrom(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}), 2)
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `345`}), 0)
	assert.Len(t, s.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `111`}), 0)
}

func TestStateSearchForBranches(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Len(t, s.SearchForBranches(`baz`), 0)
	assert.Len(t, s.SearchForBranches(`Foo bar`), 1)
	assert.Len(t, s.SearchForBranches(`a`), 2)
}

func TestStateSearchForBranch(t *testing.T) {
	t.Parallel()
	s := newTestState(t)

	b, err := s.SearchForBranch(`baz`)
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `no branch matches the specified "baz"`, err.Error())

	b, err = s.SearchForBranch(`Foo bar`)
	assert.NoError(t, err)
	assert.NotNil(t, b)
	assert.Equal(t, "Foo Bar Branch", b.ObjectName())

	b, err = s.SearchForBranch(`a`)
	assert.Error(t, err)
	assert.Nil(t, b)
	assert.Equal(t, `multiple branches match the specified "a"`, err.Error())
}

func TestStateSearchForConfigs(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	branchKey := BranchKey{Id: 123}

	assert.Len(t, s.SearchForConfigs(`baz`, branchKey), 0)
	assert.Len(t, s.SearchForConfigs(`1`, branchKey), 1)
	assert.Len(t, s.SearchForConfigs(`Config`, branchKey), 2)
}

func TestStateSearchForConfig(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	branchKey := BranchKey{Id: 123}

	c, err := s.SearchForConfig(`baz`, branchKey)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `no config matches the specified "baz"`, err.Error())

	c, err = s.SearchForConfig(`1`, branchKey)
	assert.NoError(t, err)
	assert.NotNil(t, c)
	assert.Equal(t, "Config 1", c.ObjectName())

	c, err = s.SearchForConfig(`config`, branchKey)
	assert.Error(t, err)
	assert.Nil(t, c)
	assert.Equal(t, `multiple configs match the specified "config"`, err.Error())
}

func TestStateSearchForConfigRows(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	configKey := ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}

	assert.Len(t, s.SearchForConfigRows(`baz`, configKey), 0)
	assert.Len(t, s.SearchForConfigRows(`1`, configKey), 1)
	assert.Len(t, s.SearchForConfigRows(`row`, configKey), 2)
}

func TestStateSearchForConfigRow(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	configKey := ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}

	r, err := s.SearchForConfigRow(`baz`, configKey)
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `no row matches the specified "baz"`, err.Error())

	r, err = s.SearchForConfigRow(`1`, configKey)
	assert.NoError(t, err)
	assert.NotNil(t, r)
	assert.Equal(t, "Config Row 1", r.ObjectName())

	r, err = s.SearchForConfigRow(`row`, configKey)
	assert.Error(t, err)
	assert.Nil(t, r)
	assert.Equal(t, `multiple rows match the specified "row"`, err.Error())
}

func TestStateGet(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.Equal(t, "Foo Bar Branch", s.Get(BranchKey{Id: 567}).ObjectName())
}

func TestStateGetNotFound(t *testing.T) {
	t.Parallel()
	s := newTestState(t)
	assert.PanicsWithError(t, `branch "111" not found`, func() {
		s.Get(BranchKey{Id: 111})
	})
}

func newTestState(t *testing.T) *State {
	t.Helper()
	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, `/`)
	assert.NoError(t, err)
	s := NewState(logger, fs, NewComponentsMap(nil), SortByPath)
	assert.NotNil(t, s)

	// Branch 1
	branch1Key := BranchKey{Id: 123}
	branch1State, err := s.GetOrCreate(branch1Key)
	assert.NoError(t, err)
	branch1State.SetLocalState(&Branch{
		Name:      "Main",
		IsDefault: true,
	})
	branch1State.SetManifest(&BranchManifest{BranchKey: branch1Key})

	// Branch 2
	branch2Key := BranchKey{Id: 567}
	branch2State, err := s.GetOrCreate(branch2Key)
	assert.NoError(t, err)
	branch2State.SetLocalState(&Branch{
		Name:      "Foo Bar Branch",
		IsDefault: false,
	})
	branch2State.SetManifest(&BranchManifest{BranchKey: branch2Key})

	// Config 1
	config1Key := ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: `345`}
	config1State, err := s.GetOrCreate(config1Key)
	assert.NoError(t, err)
	config1State.SetLocalState(&Config{
		Name: "Config 1",
	})
	config1State.SetManifest(&ConfigManifest{ConfigKey: config1Key})

	// Config 2
	config2Key := ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}
	config2State, err := s.GetOrCreate(config2Key)
	assert.NoError(t, err)
	config2State.SetLocalState(&Config{
		Name: "Config 2",
	})
	config2State.SetManifest(&ConfigManifest{ConfigKey: config2Key})

	// Config Row 1
	configRow1Key := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `12`}
	configRow1State, err := s.GetOrCreate(configRow1Key)
	assert.NoError(t, err)
	configRow1State.SetLocalState(&ConfigRow{
		Name: "Config Row 1",
	})
	configRow1State.SetManifest(&ConfigRowManifest{ConfigRowKey: configRow1Key})

	// Config Row 2
	configRow2Key := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `34`}
	configRow2State, err := s.GetOrCreate(configRow2Key)
	assert.NoError(t, err)
	configRow2State.SetLocalState(&ConfigRow{
		Name: "Config Row 2",
	})
	configRow2State.SetManifest(&ConfigRowManifest{ConfigRowKey: configRow2Key})

	return s
}

func TestStateMatchObjectIdOrName(t *testing.T) {
	t.Parallel()
	// Match by ID
	assert.True(t, matchObjectIdOrName(`123`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIdOrName(`1234`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc",
	}))
	assert.False(t, matchObjectIdOrName(`12`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc",
	}))

	// Match by name
	assert.True(t, matchObjectIdOrName(`abc`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIdOrName(`def`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.True(t, matchObjectIdOrName(`abc def`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
	assert.False(t, matchObjectIdOrName(`foo`, &Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Abc Def",
	}))
}
