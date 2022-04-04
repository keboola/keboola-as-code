package input

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOptions_ContainsId(t *testing.T) {
	t.Parallel()
	options := Options{Option{Value: "id1", Label: "Label 1"}, Option{Value: "id2", Label: "Label 2"}}
	assert.True(t, options.ContainsId("id1"))
	assert.True(t, options.ContainsId("id2"))
	assert.False(t, options.ContainsId("id3"))
}

func TestOptions_GetById(t *testing.T) {
	t.Parallel()

	options := Options{Option{Value: "id1", Label: "Label 1"}, Option{Value: "id2", Label: "Label 2"}}

	option, index, found := options.GetById("id1")
	assert.Equal(t, Option{Value: "id1", Label: "Label 1"}, option)
	assert.Equal(t, 0, index)
	assert.True(t, found)

	option, index, found = options.GetById("id2")
	assert.Equal(t, Option{Value: "id2", Label: "Label 2"}, option)
	assert.Equal(t, 1, index)
	assert.True(t, found)

	option, index, found = options.GetById("id3")
	assert.Equal(t, Option{}, option)
	assert.Equal(t, -1, index)
	assert.False(t, found)
}

func TestOptions_Names(t *testing.T) {
	t.Parallel()
	options := Options{Option{Value: "id1", Label: "Label 1"}, Option{Value: "id2", Label: "Label 2"}}
	assert.Equal(t, []string{"Label 1", "Label 2"}, options.Names())
}

func TestValidateDefaultOptions(t *testing.T) {
	t.Parallel()

	// Input, not select or multiselect
	assert.True(t, validateDefaultOptions("foo", KindInput, Options{}))

	// Select - invalid type, validated by other rule
	assert.True(t, validateDefaultOptions(123, KindSelect, Options{}))

	// Select - valid
	assert.True(t, validateDefaultOptions("bar", KindSelect, Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}}))

	// Select - invalid
	assert.False(t, validateDefaultOptions("abc", KindSelect, Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}}))

	// MultiSelect - invalid type, validated by other rule
	assert.True(t, validateDefaultOptions(123, KindMultiSelect, Options{}))

	// MultiSelect - valid
	assert.True(t, validateDefaultOptions(
		[]interface{}{"bar"},
		KindMultiSelect,
		Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}},
	))
	assert.True(t, validateDefaultOptions(
		[]interface{}{"bar", "foo"},
		KindMultiSelect,
		Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}},
	))

	// MultiSelect - invalid
	assert.False(t, validateDefaultOptions(
		[]interface{}{"abc"},
		KindMultiSelect,
		Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}},
	))
	assert.False(t, validateDefaultOptions(
		[]interface{}{"bar", "foo", "abc"},
		KindMultiSelect,
		Options{{Value: "foo", Label: "Foo"}, {Value: "bar", Label: "Bar"}},
	))
}
