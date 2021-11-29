package prompt

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequiredValidator(t *testing.T) {
	t.Parallel()
	assert.NoError(t, ValueRequired("abc"))
	assert.Equal(t, errors.New("value is required"), ValueRequired(""))
}
