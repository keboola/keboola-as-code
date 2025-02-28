package input

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRules_Empty(t *testing.T) {
	t.Parallel()
	assert.True(t, Rules("").Empty())
	assert.False(t, Rules("foo").Empty())
}

func TestRules_ValidateValue(t *testing.T) {
	t.Parallel()

	// Valid
	require.NoError(t, Rules("").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field"}, "foo bar"))
	require.NoError(t, Rules("required").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field"}, "foo bar"))

	// Invalid
	err := Rules("required").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field"}, "")
	require.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())

	// Invalid rule
	err = Rules("foo").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field"}, "")
	require.Error(t, err)
	assert.Equal(t, InvalidRulesError("undefined validation function 'foo'"), err)
}

func TestRules_ValidateEmptyObject(t *testing.T) {
	t.Parallel()

	// Valid
	require.NoError(t, Rules("required").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field", Type: TypeObject}, map[string]any{"foo": "bar"}))

	// Invalid
	err := Rules("required").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field", Type: TypeObject}, map[string]any{})
	require.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())

	// Invalid - multiple rules
	err = Rules("unique,required,min=1").ValidateValue(t.Context(), Input{ID: "my-field", Name: "my field", Type: TypeObject}, map[string]any{})
	require.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())
}
