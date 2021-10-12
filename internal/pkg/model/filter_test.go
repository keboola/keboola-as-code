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
