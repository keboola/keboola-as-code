package readchain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringOrType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "foo", stringOrType("foo"))
	assert.Equal(t, "bar", stringOrType(&closeFn{info: "bar"}))
	assert.Equal(t, "int", stringOrType(123))
}
