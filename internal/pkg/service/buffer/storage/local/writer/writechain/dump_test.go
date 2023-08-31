package writechain

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringOrType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "foo", stringOrType("foo"))
	assert.Equal(t, "bar", stringOrType(&flushFn{info: "bar"}))
	assert.Equal(t, "baz", stringOrType(&closeFn{info: "baz"}))
	assert.Equal(t, "int", stringOrType(123))
}
