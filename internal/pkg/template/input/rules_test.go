package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRules_Empty(t *testing.T) {
	t.Parallel()
	assert.True(t, Rules("").Empty())
	assert.False(t, Rules("foo").Empty())
}

func TestRules_ValidateValue(t *testing.T) {
	t.Parallel()

	// Valid
	assert.NoError(t, Rules("").ValidateValue("foo bar", "my-field"))
	assert.NoError(t, Rules("required").ValidateValue("foo bar", "my-field"))

	// Invalid
	err := Rules("required").ValidateValue("", "my-field")
	assert.Error(t, err)
	assert.Equal(t, "my-field is a required field", err.Error())

	// Invalid rule
	err = Rules("foo").ValidateValue("", "my-field")
	assert.Error(t, err)
	assert.Equal(t, InvalidRulesError("undefined validation function 'foo'"), err)
}
