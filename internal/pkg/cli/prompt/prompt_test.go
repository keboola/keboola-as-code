package prompt

import (
	"errors"
	"testing"

	"github.com/AlecAivazis/survey/v2/core"
	"github.com/stretchr/testify/assert"
)

func TestRequiredValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ValueRequired("abc"))
	assert.Equal(t, errors.New("value is required"), ValueRequired(""))
	assert.Equal(t, errors.New("value is required"), ValueRequired("\t"))
	assert.Equal(t, errors.New("value is required"), ValueRequired(" "))
}

func TestAtLeastOneRequired(t *testing.T) {
	t.Parallel()
	assert.NoError(t, AtLeastOneRequired([]core.OptionAnswer{{Index: 123, Value: `abc`}}))
	assert.Equal(t, errors.New("at least one value is required"), AtLeastOneRequired([]core.OptionAnswer{}))
}
