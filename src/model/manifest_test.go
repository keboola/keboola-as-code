package model

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestIsBranchAllowed(t *testing.T) {
	assert.True(t, (AllowedBranches{"*", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.True(t, (AllowedBranches{"123", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.True(t, (AllowedBranches{"abc", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.True(t, (AllowedBranches{"a*", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.True(t, (AllowedBranches{"a?c", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.True(t, (AllowedBranches{"*c", "xyz"}).IsBranchAllowed(123, "abc"))

	assert.False(t, (AllowedBranches{"12*", "xyz"}).IsBranchAllowed(123, "abc"))
	assert.False(t, (AllowedBranches{"abcdef", "xyz"}).IsBranchAllowed(123, "abc"))
}
