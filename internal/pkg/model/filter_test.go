package model_test

import (
	"slices"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestAllowedBranches_IsOneSpecificBranch(t *testing.T) {
	t.Parallel()
	assert.False(t, (AllowedBranches{}).IsOneSpecificBranch())
	assert.False(t, (AllowedBranches{AllBranchesDef}).IsOneSpecificBranch())
	assert.False(t, (AllowedBranches{"123", "456"}).IsOneSpecificBranch())
	assert.False(t, (AllowedBranches{"foo-*"}).IsOneSpecificBranch())
	assert.False(t, (AllowedBranches{"foo-???"}).IsOneSpecificBranch())
	assert.True(t, (AllowedBranches{"foo"}).IsOneSpecificBranch())
	assert.True(t, (AllowedBranches{"123"}).IsOneSpecificBranch())
}

func TestIsBranchAllowed(t *testing.T) {
	t.Parallel()
	assert.True(t, (AllowedBranches{"*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"123", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"abc", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a?c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"*c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: true}),
	)

	assert.False(t, (AllowedBranches{"12*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{"abcdef", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "abc", IsDefault: false}),
	)
}

func TestComponentsIds(t *testing.T) {
	t.Parallel()
	ids := ComponentIDs{"foo", "bar"}
	assert.True(t, slices.Contains(ids, "foo"))
	assert.True(t, slices.Contains(ids, "bar"))
	assert.False(t, slices.Contains(ids, "baz"))
}

func TestFilterIsObjectIgnored(t *testing.T) {
	t.Parallel()
	m := NewFilter(
		AllowedBranches{"dev-*", "123", "abc"},
		ComponentIDs{"aaa", "bbb"},
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{ID: 789}, Name: "dev-1"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{ID: 123}, Name: "xyz"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{ID: 789}, Name: "abc"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{ID: 789}, Name: "xyz"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentID: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentID: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentID: "ccc"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentID: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentID: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentID: "ccc"}}),
	)
}

func TestAlwaysIgnoredComponents(t *testing.T) {
	t.Parallel()
	m := NoFilter()
	assert.True(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentID: keboola.SandboxWorkspacesComponent}},
	))
}

func TestObjectsFilter_SetAllowedKeys(t *testing.T) {
	t.Parallel()

	object1 := &fixtures.MockedObject{
		MockedKey: fixtures.MockedKey{
			ID: "123",
		},
	}
	object2 := &fixtures.MockedObject{
		MockedKey: fixtures.MockedKey{
			ID: "456",
		},
	}

	f := NoFilter()
	assert.False(t, f.IsObjectIgnored(object1))
	assert.False(t, f.IsObjectIgnored(object2))

	f.SetAllowedKeys([]Key{object1.Key()})
	assert.False(t, f.IsObjectIgnored(object1))
	assert.True(t, f.IsObjectIgnored(object2))

	f.SetAllowedKeys([]Key{object1.Key(), object2.Key()})
	assert.False(t, f.IsObjectIgnored(object1))
	assert.False(t, f.IsObjectIgnored(object2))
}
