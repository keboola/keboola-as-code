package input

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKind_IsValid(t *testing.T) {
	t.Parallel()
	assert.True(t, slices.Contains(allKinds(), KindInput))
	assert.True(t, slices.Contains(allKinds(), KindHidden))
	assert.True(t, slices.Contains(allKinds(), KindTextarea))
	assert.True(t, slices.Contains(allKinds(), KindConfirm))
	assert.True(t, slices.Contains(allKinds(), KindSelect))
	assert.True(t, slices.Contains(allKinds(), KindMultiSelect))
	assert.True(t, slices.Contains(allKinds(), KindOAuth))
	assert.False(t, slices.Contains(allKinds(), Kind("foo")))
}

func TestKind_ValidateType(t *testing.T) {
	t.Parallel()
	// Input
	require.NoError(t, KindInput.ValidateType(TypeString))
	require.NoError(t, KindInput.ValidateType(TypeInt))
	require.NoError(t, KindInput.ValidateType(TypeDouble))
	err := KindInput.ValidateType(TypeBool)
	require.Error(t, err)
	assert.Equal(t, "should be one of [string, int, double] for kind=input, found bool", err.Error())
	err = KindInput.ValidateType(TypeStringArray)
	require.Error(t, err)
	assert.Equal(t, "should be one of [string, int, double] for kind=input, found string[]", err.Error())

	// Hidden
	require.NoError(t, KindHidden.ValidateType(TypeString))
	err = KindHidden.ValidateType(TypeInt)
	require.Error(t, err)
	assert.Equal(t, "should be string for kind=hidden, found int", err.Error())

	// Confirm
	require.NoError(t, KindConfirm.ValidateType(TypeBool))
	err = KindConfirm.ValidateType(TypeInt)
	require.Error(t, err)
	assert.Equal(t, "should be bool for kind=confirm, found int", err.Error())

	// Select
	require.NoError(t, KindSelect.ValidateType(TypeString))
	err = KindSelect.ValidateType(TypeStringArray)
	require.Error(t, err)
	assert.Equal(t, "should be string for kind=select, found string[]", err.Error())

	// MultiSelect
	require.NoError(t, KindMultiSelect.ValidateType(TypeStringArray))
	err = KindMultiSelect.ValidateType(TypeString)
	require.Error(t, err)
	assert.Equal(t, "should be string[] for kind=multiselect, found string", err.Error())

	// oAuth
	require.NoError(t, KindOAuth.ValidateType(TypeObject))
	err = KindOAuth.ValidateType(TypeString)
	require.Error(t, err)
	assert.Equal(t, "should be object for kind=oauth, found string", err.Error())

	// oAuthAccounts
	require.NoError(t, KindOAuthAccounts.ValidateType(TypeObject))
	err = KindOAuthAccounts.ValidateType(TypeString)
	require.Error(t, err)
	assert.Equal(t, "should be object for kind=oauthAccounts, found string", err.Error())
}
