package orchestrator

import (
	"fmt"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestParsePhase(t *testing.T) {
	t.Parallel()

	cases := []struct {
		before, after string
		callback      func(p *phaseParser) (any, error)
		expected      any
		err           error
	}{
		{
			`{"id":123,"foo":"bar"}`,
			`{"id":123,"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.id() },
			"",
			errors.New(`"id" must be string, found 123`),
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.id() },
			"",
			errors.New(`missing "id" key`),
		},
		{
			`{"id":12.34,"foo":"bar"}`,
			`{"id":12.34,"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.id() },
			"",
			errors.New(`"id" must be string, found 12.34`),
		},
		{
			`{"id":"123","foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.id() },
			"123",
			nil,
		},
		{
			`{"id":"","foo":"bar"}`,
			`{"id":"","foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.id() },
			"",
			errors.New(`"id" cannot be empty`),
		},
		{
			`{"name":"phase", "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.name() },
			`phase`,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.name() },
			"",
			errors.New(`missing "name" key`),
		},
		{
			`{"name":"","foo":"bar"}`,
			`{"name":"","foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.name() },
			``,
			errors.New(`"name" cannot be empty`),
		},
		{
			`{"name":123,"foo":"bar"}`,
			`{"name":123,"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.name() },
			``,
			errors.New(`"name" must be string, found float64`),
		},
		{
			`{"dependsOn":[],"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnIds() },
			[]string{},
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnIds() },
			[]string{},
			nil,
		},
		{
			`{"dependsOn":["1", "2", "3"], "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnIds() },
			[]string{"1", "2", "3"},
			nil,
		},
		{
			`{"dependsOn":[1,"2","3"],"foo":"bar"}`,
			`{"dependsOn":[1,"2","3"],"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnIds() },
			[]string(nil),
			errors.New(`"dependsOn" key must contain only strings, found 1, index 0`),
		},
		{
			`{"dependsOn":["foo1", "foo2"],"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnPaths() },
			[]string{"foo1", "foo2"},
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnPaths() },
			[]string{},
			nil,
		},
		{
			`{"dependsOn":[], "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnPaths() },
			[]string{},
			nil,
		},
		{
			`{"dependsOn":["1",2,"3"],"foo":"bar"}`,
			`{"dependsOn":["1",2,"3"],"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.dependsOnPaths() },
			[]string(nil),
			errors.New(`"dependsOn" key must contain only strings, found string, index 1`),
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *phaseParser) (any, error) { return p.additionalContent(), nil },
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
		assert.Equal(t, c.after, json.MustEncodeString(p.content, false), desc)
		if c.err == nil {
			require.NoError(t, err, desc)
		} else {
			require.Error(t, err, desc)
			assert.Equal(t, c.err.Error(), err.Error(), desc)
		}
	}
}
