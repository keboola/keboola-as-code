package orchestrator

import (
	"fmt"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestParseTask(t *testing.T) {
	t.Parallel()

	cases := []struct {
		before, after string
		callback      func(p *taskParser) (any, error)
		expected      any
		err           error
	}{
		{
			`{"id":123, "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.id() },
			123,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.id() },
			0,
			errors.New(`missing "id" key`),
		},
		{
			`{"id":12.34,"foo":"bar"}`,
			`{"id":12.34,"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.id() },
			0,
			errors.New(`"id" must be int, found "12.34"`),
		},
		{
			`{"id":"123","foo":"bar"}`,
			`{"id":"123","foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.id() },
			0,
			errors.New(`"id" must be int, found string`),
		},
		{
			`{"id":"","foo":"bar"}`,
			`{"id":"","foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.id() },
			0,
			errors.New(`"id" must be int, found string`),
		},
		{
			`{"name":"phase", "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.name() },
			`phase`,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.name() },
			"",
			errors.New(`missing "name" key`),
		},
		{
			`{"name":"","foo":"bar"}`,
			`{"name":"","foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.name() },
			``,
			errors.New(`"name" cannot be empty`),
		},
		{
			`{"name":123,"foo":"bar"}`,
			`{"name":123,"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.name() },
			``,
			errors.New(`"name" must be string, found float64`),
		},
		{
			`{"enabled":true, "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.enabled() },
			true,
			nil,
		},
		{
			`{"enabled":false, "foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.enabled() },
			false,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.enabled() },
			true, // true is default value if key is missing
			nil,
		},
		{
			`{"enabled":12.34,"foo":"bar"}`,
			`{"enabled":12.34,"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.enabled() },
			true, // true is default value
			errors.New(`"enabled" must be boolean, found float64`),
		},
		{
			`{"phase":123,"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.phaseID() },
			123,
			nil,
		},
		{
			`{"phase":"123","foo":"bar"}`,
			`{"phase":"123","foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.phaseID() },
			0,
			errors.New(`"phase" must be int, found string`),
		},
		{
			`{"phase":"","foo":"bar"}`,
			`{"phase":"","foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.phaseID() },
			0,
			errors.New(`"phase" must be int, found string`),
		},
		{
			`{"task":{"componentId":"foo.bar", "mode":"run"}, "foo":"bar"}`,
			`{"task":{"mode":"run"},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.componentID() },
			keboola.ComponentID(`foo.bar`),
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.componentID() },
			keboola.ComponentID(""),
			errors.New(`missing "task" key`),
		},
		{
			`{"task":{},"foo":"bar"}`,
			`{"task":{},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.componentID() },
			keboola.ComponentID(""),
			errors.New(`missing "task.componentId" key`),
		},
		{
			`{"task":{"componentId":""},"foo":"bar"}`,
			`{"task":{"componentId":""},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.componentID() },
			keboola.ComponentID(``),
			errors.New(`"task.componentId" cannot be empty`),
		},
		{
			`{"task":{"componentId":123},"foo":"bar"}`,
			`{"task":{"componentId":123},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.componentID() },
			keboola.ComponentID(``),
			errors.New(`"task.componentId" must be string, found float64`),
		},
		{
			`{"task":{"configId":"foo.bar", "mode":"run"}, "foo":"bar"}`,
			`{"task":{"mode":"run"},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configID() },
			keboola.ConfigID(`foo.bar`),
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configID() },
			keboola.ConfigID(""),
			errors.New(`missing "task" key`),
		},
		{
			`{"task":{},"foo":"bar"}`,
			`{"task":{},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configID() },
			keboola.ConfigID(""),
			errors.New(`missing "task.configId" key`),
		},
		{
			`{"task":{"configId":""},"foo":"bar"}`,
			`{"task":{"configId":""},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configID() },
			keboola.ConfigID(``),
			errors.New(`"task.configId" cannot be empty`),
		},
		{
			`{"task":{"configId":123},"foo":"bar"}`,
			`{"task":{"configId":123},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configID() },
			keboola.ConfigID(``),
			errors.New(`"task.configId" must be string, found float64`),
		},
		{
			`{"task":{"configPath":"foo", "mode":"run"}, "foo":"bar"}`,
			`{"task":{"mode":"run"},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configPath() },
			`foo`,
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configPath() },
			"",
			errors.New(`missing "task" key`),
		},
		{
			`{"task":{},"foo":"bar"}`,
			`{"task":{},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configPath() },
			"",
			errors.New(`missing "task.configPath" key`),
		},
		{
			`{"task":{"configPath":""},"foo":"bar"}`,
			`{"task":{"configPath":""},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configPath() },
			``,
			errors.New(`"task.configPath" cannot be empty`),
		},
		{
			`{"task":{"configPath":123},"foo":"bar"}`,
			`{"task":{"configPath":123},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configPath() },
			``,
			errors.New(`"task.configPath" must be string, found float64`),
		},
		{
			`{"task":{"configData":{"params":"value"}, "mode":"run"}, "foo":"bar"}`,
			`{"task":{"mode":"run"},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configData() },
			orderedmap.FromPairs([]orderedmap.Pair{{Key: "params", Value: "value"}}),
			nil,
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configData() },
			(*orderedmap.OrderedMap)(nil),
			errors.New(`missing "task" key`),
		},
		{
			`{"task":{},"foo":"bar"}`,
			`{"task":{},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configData() },
			(*orderedmap.OrderedMap)(nil),
			errors.New(`missing "task.configData" key`),
		},
		{
			`{"task":{"configData":"abc"},"foo":"bar"}`,
			`{"task":{"configData":"abc"},"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.configData() },
			(*orderedmap.OrderedMap)(nil),
			errors.New(`"task.configData" must be object, found string`),
		},
		{
			`{"foo":"bar"}`,
			`{"foo":"bar"}`,
			func(p *taskParser) (any, error) { return p.additionalContent(), nil },
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

		p := &taskParser{content: content}
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
