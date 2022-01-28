package deepcopy_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	. "github.com/keboola/keboola-as-code/internal/pkg/utils/deepcopy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testassert"
)

type Values []*Bar

type Foo struct {
	Values Values
}

type Bar struct {
	Key1 string
	Key2 string
	Key3 interface{} // nil interface
}

type UnExportedFields struct {
	key1 string
	key2 string
}

func TestCopy(t *testing.T) {
	t.Parallel()
	original := testValue()
	clone := Copy(original)
	assert.Equal(t, original, clone)
	assert.NotSame(t, original, clone)
	testassert.DeepEqualNotSame(t, original, clone, ``)
}

func TestCopyWithTranslate(t *testing.T) {
	t.Parallel()
	original := testValue()
	clone := CopyTranslate(original, func(_, clone reflect.Value, _ Steps) {
		// Modify all strings
		if clone.Kind() == reflect.String {
			clone.Set(reflect.ValueOf(clone.Interface().(string) + `_modified`))
		}
	})
	expected := `
{
  "foo": {
    "Values": [
      {
        "Key1": "value1_modified",
        "Key2": "value2_modified",
        "Key3": null
      },
      {
        "Key1": "value3_modified",
        "Key2": "value4_modified",
        "Key3": null
      }
    ]
  },
  "bar": {
    "Key1": "value1_modified",
    "Key2": "value2_modified",
    "Key3": null
  },
  "[]empty": null,
  "[]bar": [
    {
      "Key1": "value1_modified",
      "Key2": "value2_modified",
      "Key3": null
    },
    {
      "Key1": "value1_modified",
      "Key2": "value2_modified",
      "Key3": null
    }
  ],
  "subMap": {
    "key1": 123,
    "key2": 456
  }
}
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), json.MustEncodeString(clone, true))
}

func TestCopyWithTranslateSteps(t *testing.T) {
	t.Parallel()
	original := testValue()
	clone := CopyTranslate(original, func(_, clone reflect.Value, steps Steps) {
		// Modify all strings
		if clone.Kind() == reflect.String {
			clone.Set(reflect.ValueOf(steps.String()))
		}
	})
	expected := `
{
  "foo": {
    "Values": [
      {
        "Key1": "*orderedmap.OrderedMap[foo].*deepcopy_test.Foo[Values].slice[0].*deepcopy_test.Bar[Key1].string",
        "Key2": "*orderedmap.OrderedMap[foo].*deepcopy_test.Foo[Values].slice[0].*deepcopy_test.Bar[Key2].string",
        "Key3": null
      },
      {
        "Key1": "*orderedmap.OrderedMap[foo].*deepcopy_test.Foo[Values].slice[1].*deepcopy_test.Bar[Key1].string",
        "Key2": "*orderedmap.OrderedMap[foo].*deepcopy_test.Foo[Values].slice[1].*deepcopy_test.Bar[Key2].string",
        "Key3": null
      }
    ]
  },
  "bar": {
    "Key1": "*orderedmap.OrderedMap[bar].deepcopy_test.Bar[Key1].string",
    "Key2": "*orderedmap.OrderedMap[bar].deepcopy_test.Bar[Key2].string",
    "Key3": null
  },
  "[]empty": null,
  "[]bar": [
    {
      "Key1": "*orderedmap.OrderedMap[[]bar].slice[0].interface[deepcopy_test.Bar].deepcopy_test.Bar[Key1].string",
      "Key2": "*orderedmap.OrderedMap[[]bar].slice[0].interface[deepcopy_test.Bar].deepcopy_test.Bar[Key2].string",
      "Key3": null
    },
    {
      "Key1": "*orderedmap.OrderedMap[[]bar].slice[1].interface[deepcopy_test.Bar].deepcopy_test.Bar[Key1].string",
      "Key2": "*orderedmap.OrderedMap[[]bar].slice[1].interface[deepcopy_test.Bar].deepcopy_test.Bar[Key2].string",
      "Key3": null
    }
  ],
  "subMap": {
    "key1": 123,
    "key2": 456
  }
}
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), json.MustEncodeString(clone, true))
}

func TestCopyCycle(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`key`, m)
	expected := `
deepcopy cycle detected
each pointer can be used only once
steps: *orderedmap.OrderedMap[key]
`
	assert.PanicsWithError(t, strings.TrimSpace(expected), func() {
		Copy(m)
	})
}

func TestCopyUnexportedFields(t *testing.T) {
	t.Parallel()
	m := orderedmap.New()
	m.Set(`key`, &UnExportedFields{key1: `a`, key2: `b`})
	expected := `
deepcopy found unexported field
steps: *orderedmap.OrderedMap[key].*deepcopy_test.UnExportedFields[key1]
value: deepcopy_test.UnExportedFields{key1:"a", key2:"b"}
`
	assert.PanicsWithError(t, strings.TrimSpace(expected), func() {
		Copy(m)
	})
}

func testValue() interface{} {
	m := orderedmap.New()
	m.Set(`foo`, &Foo{
		Values: []*Bar{
			{
				Key1: `value1`,
				Key2: `value2`,
			},
			{
				Key1: `value3`,
				Key2: `value4`,
			},
		},
	})
	m.Set(`bar`, Bar{
		Key1: `value1`,
		Key2: `value2`,
	})
	m.Set(`[]empty`, []interface{}(nil))
	m.Set(`[]bar`, []interface{}{
		Bar{
			Key1: `value1`,
			Key2: `value2`,
		},
		Bar{
			Key1: `value1`,
			Key2: `value2`,
		},
	})

	subMap := orderedmap.New()
	subMap.Set(`key1`, 123)
	subMap.Set(`key2`, 456)
	m.Set(`subMap`, subMap)

	return m
}
