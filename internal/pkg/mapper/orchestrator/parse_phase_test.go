package orchestrator

import (
	"fmt"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
)

func TestParsePhase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		before, after string
		callback      func(p *phaseParser) (interface{}, error)
		expected      interface{}
		err           error
	}{
		{
			`{"id":123, "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.id() },
			123,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.id() },
			0,
			fmt.Errorf(`missing "id" key`),
		},
		{
			`{"id":12.34,"foo":"bar"}`,
			`{"id":12.34,"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.id() },
			0,
			fmt.Errorf(`"id" must be int, found "12.34"`),
		},
		{
			`{"id":"123","foo":"bar"}`,
			`{"id":"123","foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.id() },
			0,
			fmt.Errorf(`"id" must be int, found string`),
		},
		{
			`{"id":"","foo":"bar"}`,
			`{"id":"","foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.id() },
			0,
			fmt.Errorf(`"id" must be int, found string`),
		},
		{
			`{"name":"phase", "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.name() },
			`phase`,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.name() },
			"",
			fmt.Errorf(`missing "name" key`),
		},
		{
			`{"name":"","foo":"bar"}`,
			`{"name":"","foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.name() },
			``,
			fmt.Errorf(`"name" cannot be empty`),
		},
		{
			`{"name":123,"foo":"bar"}`,
			`{"name":123,"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.name() },
			``,
			fmt.Errorf(`"name" must be string, found float64`),
		},
		{
			`{"dependsOn":[],"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnIds() },
			[]int{},
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnIds() },
			[]int{},
			nil,
		},
		{
			`{"dependsOn":[1, 2, 3], "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnIds() },
			[]int{1, 2, 3},
			nil,
		},
		{
			`{"dependsOn":[1,"2",3],"foo":"bar"}`,
			`{"dependsOn":[1,"2",3],"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnIds() },
			[]int(nil),
			fmt.Errorf(`"dependsOn" key must contain only integers, found "2", index 1`),
		},
		{
			`{"dependsOn":["foo1", "foo2"],"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnPaths() },
			[]string{"foo1", "foo2"},
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnPaths() },
			[]string{},
			nil,
		},
		{
			`{"dependsOn":[], "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnPaths() },
			[]string{},
			nil,
		},
		{
			`{"dependsOn":["1",2,"3"],"foo":"bar"}`,
			`{"dependsOn":["1",2,"3"],"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.dependsOnPaths() },
			[]string(nil),
			fmt.Errorf(`"dependsOn" key must contain only strings, found string, index 1`),
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (interface{}, error) { return p.additionalContent(), nil },
			orderedmap.FromPairs([]orderedmap.Pair{
				{Key: `foo`, Value: `bar`},
			}),
			nil,
		},
	}

	for i, c := range cases {
		desc := fmt.Sprintf(`Case "%d"`, i+1)
		content := orderedmap.New()
		json.MustDecodeString(c.before, content)

		p := &phaseParser{content: content}
		value, err := c.callback(p)

		assert.Equal(t, c.expected, value, desc)
		assert.Equal(t, c.err, err, desc)
		assert.Equal(t, c.after, json.MustEncodeString(p.content, false), desc)
	}
}
