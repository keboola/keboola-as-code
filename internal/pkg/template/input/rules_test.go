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
	assert.NoError(t, Rules("").ValidateValue(Input{Id: "my-field", Name: "my field"}, "foo bar"))
	assert.NoError(t, Rules("required").ValidateValue(Input{Id: "my-field", Name: "my field"}, "foo bar"))

	// Invalid
	err := Rules("required").ValidateValue(Input{Id: "my-field", Name: "my field"}, "")
	assert.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())

	// Invalid rule
	err = Rules("foo").ValidateValue(Input{Id: "my-field", Name: "my field"}, "")
	assert.Error(t, err)
	assert.Equal(t, InvalidRulesError("undefined validation function 'foo'"), err)
}

func TestRules_ValidateEmptyObject(t *testing.T) {
	t.Parallel()

	// Valid
	assert.NoError(t, Rules("required").ValidateValue(Input{Id: "my-field", Name: "my field", Type: TypeObject}, map[string]any{"foo": "bar"}))

	// Invalid
	err := Rules("required").ValidateValue(Input{Id: "my-field", Name: "my field", Type: TypeObject}, map[string]any{})
	assert.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())

	// Invalid - multiple rules
	err = Rules("unique,required,min=1").ValidateValue(Input{Id: "my-field", Name: "my field", Type: TypeObject}, map[string]any{})
	assert.Error(t, err)
	assert.Equal(t, "my field is a required field", err.Error())
}
