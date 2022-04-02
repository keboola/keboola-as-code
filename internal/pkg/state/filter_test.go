package state_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/state"
)

func TestIsBranchAllowed(t *testing.T) {
	t.Parallel()
	assert.True(t, (AllowedBranches{"*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"123", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"abc", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"a?c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{"*c", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.True(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: true}),
	)

	assert.False(t, (AllowedBranches{"12*", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{"abcdef", "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
	assert.False(t, (AllowedBranches{MainBranchDef, "xyz"}).IsBranchAllowed(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "abc", IsDefault: false}),
	)
}

func TestBaseFilter_IsObjectIgnored(t *testing.T) {
	t.Parallel()
	m := NewBaseFilter()
	m.SetAllowedBranches(AllowedBranches{"dev-*", "123", "abc"})
	m.SetIgnoredComponents(ComponentIds{"aaa", "bbb"})

	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{BranchId: 789}, Name: "dev-1"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{BranchId: 123}, Name: "xyz"}),
	)
	assert.False(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{BranchId: 789}, Name: "abc"}),
	)
	assert.True(t, m.IsObjectIgnored(
		&Branch{BranchKey: BranchKey{BranchId: 789}, Name: "xyz"}),
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
		&ConfigRow{ConfigRowKey: ConfigRowKey{ConfigKey: ConfigKey{ComponentId: "aaa"}}}),
	)
	assert.True(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ConfigKey: ConfigKey{ComponentId: "bbb"}}}),
	)
	assert.False(t, m.IsObjectIgnored(
		&ConfigRow{ConfigRowKey: ConfigRowKey{ConfigKey: ConfigKey{ComponentId: "ccc"}}}),
	)
}

func TestAllowedKeysFilter(t *testing.T) {
	t.Parallel()
	object1 := fixtures.MockedObject{
		MockedKey: fixtures.MockedKey{
			Id: "123",
		},
	}
	object2 := fixtures.MockedObject{
		MockedKey: fixtures.MockedKey{
			Id: "456",
		},
	}

	f := NewAllowedKeysFilter()
	assert.True(t, f.IsObjectIgnored(object1))
	assert.True(t, f.IsObjectIgnored(object2))

	f.SetAllowedKeys(object1.Key())
	assert.False(t, f.IsObjectIgnored(object1))
	assert.True(t, f.IsObjectIgnored(object2))

	f.SetAllowedKeys(object1.Key(), object2.Key())
	assert.False(t, f.IsObjectIgnored(object1))
	assert.False(t, f.IsObjectIgnored(object2))
}

func TestComposedFilter_IsObjectIgnored(t *testing.T) {
	f1 := NewBaseFilter()
	f1.SetIgnoredComponents(ComponentIds{"foo.bar1"})
	f2 := NewBaseFilter()
	f2.SetIgnoredComponents(ComponentIds{"foo.bar2"})
	f := NewComposedFilter(f1, f2)

	assert.True(t, f.IsObjectIgnored(&Config{ConfigKey: ConfigKey{ComponentId: "foo.bar1"}}))
	assert.True(t, f.IsObjectIgnored(&Config{ConfigKey: ConfigKey{ComponentId: "foo.bar2"}}))
	assert.False(t, f.IsObjectIgnored(&Config{ConfigKey: ConfigKey{ComponentId: "foo.bar3"}}))
}
