package prompt

import (
	"testing"

	"github.com/AlecAivazis/survey/v2/core"
	"github.com/stretchr/testify/assert"
)

func TestRequiredValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ValueRequired("abc"))
	assert.Equal(t, "value is required", ValueRequired("").Error())
	assert.Equal(t, "value is required", ValueRequired("\t").Error())
	assert.Equal(t, "value is required", ValueRequired(" ").Error())
}

func TestAtLeastOneRequired(t *testing.T) {
	t.Parallel()
	assert.NoError(t, AtLeastOneRequired([]core.OptionAnswer{{Index: 123, Value: `abc`}}))
	assert.Equal(t, "at least one value is required", AtLeastOneRequired([]core.OptionAnswer{}).Error())
}
