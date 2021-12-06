package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestUpdateMapStep(t *testing.T) {
	t.Parallel()
	content := PairsToOrderedMap([]Pair{
		{
			Key:   "key1",
			Value: "value1",
		},
		{
			Key:   "key2",
			Value: "value1",
		},
		{
			Key: "parameters",
			Value: *PairsToOrderedMap([]Pair{
				{
					Key:   "host",
					Value: "mysql.example.com",
				},
			}),
		},
	})
	path := KeyPath{MapStep("parameters"), MapStep("host")}
	content = UpdateIn(content, path, "newValue")
	parameters, _ := content.Get("parameters")
	p := parameters.(orderedmap.OrderedMap)
	host, _ := p.Get("host")
	assert.Equal(t, host, "newValue")
}

func TestUpdateSliceStep(t *testing.T) {
	t.Parallel()
	content := PairsToOrderedMap([]Pair{
		{
			Key:   "key1",
			Value: "value1",
		},
		{
			Key:   "key2",
			Value: "value1",
		},
		{
			Key: "parameters",
			Value: *PairsToOrderedMap([]Pair{
				{
					Key:   "host",
					Value: "mysql.example.com",
				},
				{
					Key: "values",
					Value: []interface{}{
						*PairsToOrderedMap([]Pair{
							{
								Key:   "name",
								Value: "john",
							},
						}),
						*PairsToOrderedMap([]Pair{
							{
								Key:   "name",
								Value: "kate",
							},
						}),
					},
				},
			}),
		},
	})
	path := KeyPath{MapStep("parameters"), MapStep("values"), SliceStep(1), MapStep("name")}
	content = UpdateIn(content, path, "newValue")
	parameters, _ := content.Get("parameters")
	parametersMap := parameters.(orderedmap.OrderedMap)
	values, _ := parametersMap.Get("values")
	secondName := values.([]interface{})[1]
	secondNameMap := secondName.(orderedmap.OrderedMap)
	name, _ := secondNameMap.Get("name")
	assert.Equal(t, name, "newValue")
}

func TestGetFromMap(t *testing.T) {
	t.Parallel()
	input := `
{
  "foo": {
    "bar": {
      "baz": {
        "value": 123
      }
    }
  }
}
`
	m := NewOrderedMap()
	json.MustDecodeString(input, m)
	cases := []struct {
		keys     []string
		expected interface{}
	}{
		{[]string{}, `{"foo":{"bar":{"baz":{"value":123}}}}`},
		{[]string{`foo1`}, nil},
		{[]string{`foo1`, `foo2`}, nil},
		{[]string{`foo`}, `{"bar":{"baz":{"value":123}}}`},
		{[]string{`foo`, `bar`}, `{"baz":{"value":123}}`},
		{[]string{`foo`, `bar`, `baz`}, `{"value":123}`},
		{[]string{`foo`, `bar`, `baz`, `value`}, `123`},
		{[]string{`foo`, `bar`, `baz`, `value`, `xyz`}, nil},
	}

	for i, c := range cases {
		v := GetFromMap(m, c.keys)
		if v != nil {
			v = json.MustEncodeString(v, false)
		}
		assert.Equal(t, c.expected, v, fmt.Sprintf(`case "%d"`, i))
	}
}
