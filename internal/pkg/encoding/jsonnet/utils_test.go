package jsonnet

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
)

func TestValueToJSONType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 123, ValueToJSONType(123))
	assert.Equal(t, "abc", ValueToJSONType("abc"))
	assert.Equal(t,
		map[string]any{"key": "value"},
		ValueToJSONType(orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "value"},
		})),
	)
}
