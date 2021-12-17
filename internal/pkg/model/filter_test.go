package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsBranchAllowed(t *testing.T) {
	t.Parallel()
	assert.True(t, (AllowedBranches{"*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"123", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"abc", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a?c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"*c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: true}),
	)

	assert.False(t, (AllowedBranches{"12*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{"abcdef", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "abc", IsDefault: false}),
	)
}

func TestComponentsIds(t *testing.T) {
	t.Parallel()
	ids := ComponentIds{"foo", "bar"}
	assert.True(t, ids.Contains("foo"))
	assert.True(t, ids.Contains("bar"))
	assert.False(t, ids.Contains("baz"))
}

func TestFilterIsObjectIgnored(t *testing.T) {
	t.Parallel()
	m := Filter{
		AllowedBranches:   AllowedBranches{"dev-*", "123", "abc"},
		IgnoredComponents: ComponentIds{"aaa", "bbb"},
	}
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{Id: 789}, Name: "dev-1"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{Id: 123}, Name: "xyz"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{Id: 789}, Name: "abc"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{Id: 789}, Name: "xyz"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentId: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentId: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Config{ConfigKey: ConfigKey{ComponentId: "ccc"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentId: "aaa"}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentId: "bbb"}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ComponentId: "ccc"}}),
	)
}
