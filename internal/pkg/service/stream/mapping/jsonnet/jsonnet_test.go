package jsonnet

import (
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBufferJsonnetFunctions(t *testing.T) {
	t.Parallel()
	now1, err := time.Parse(`2006-01-02T15:04:05.999999Z07:00`, `2006-01-01T15:04:05.123456+07:00`)
	assert.NoError(t, err)
	now2, err := time.Parse(`2006-01-02T15:04:05.999999Z07:00`, `2006-01-01T15:04:05.000000+07:00`)
	assert.NoError(t, err)

	cases := []testCase{
		// Ip()
		{
			name:     "Ip function",
			template: "Ip()",
			result:   `"1.2.3.4"`,
			reqCtx:   &recordctx.Context{IP: net.ParseIP("1.2.3.4")},
		},
		// Now()
		{
			name:     "Now function",
			template: "Now()",
			result:   `"2006-01-01T08:04:05.123Z"`,
			reqCtx:   &recordctx.Context{Now: now1},
		},
		{
			name:     "Now function with zero milliseconds",
			template: "Now()",
			result:   `"2006-01-01T08:04:05.000Z"`,
			reqCtx:   &recordctx.Context{Now: now2},
		},
		{
			name:     "Now function with a custom format",
			template: "Now('%Y-%m-%d')",
			result:   `"2006-01-01"`,
			reqCtx:   &recordctx.Context{Now: now1},
		},
		// HeaderStr()
		{
			name:     "HeaderStr function - no header",
			template: "HeaderStr()",
			result:   `""`,
			reqCtx:   &recordctx.Context{},
		},
		{
			name:     "HeaderStr function - all",
			template: "HeaderStr()",
			result:   `"X-Header-1: value1\nX-Header-2: value2\n"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"X-Header-2": []string{"value2"}, "X-Header-1": []string{"value1"}}},
		},
		// BodyStr()
		{
			name:     "BodyStr function - empty",
			template: "BodyStr()",
			result:   `""`,
			reqCtx:   &recordctx.Context{Body: ""},
		},
		{
			name:     "BodyStr function - not empty",
			template: "BodyStr()",
			result:   `"foo"`,
			reqCtx:   &recordctx.Context{Body: "foo"},
		},
		// Header(name, default)
		{
			name:     "Header function - no header",
			template: "Header()",
			result:   `{}`,
			reqCtx:   &recordctx.Context{},
		},
		{
			name:     "Header function - all",
			template: "Header()",
			result:   `{"X-Header-1":"value1","X-Header-2":"value2"}`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"X-Header-2": []string{"value2"}, "X-Header-1": []string{"value1"}}},
		},
		{
			name:     "Header function - one",
			template: "Header('x-header')",
			result:   `"value"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"X-Header": []string{"value"}}},
		},
		{
			name:     "Header function - one, default value",
			template: "Header('x-header', 'default value')",
			result:   `"value"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"X-Header": []string{"value"}}},
		},
		{
			name:     "Header function - one, not found",
			template: "Header('x-header-3')",
			err:      `header "X-Header-3" not found`,
			reqCtx:   &recordctx.Context{Headers: http.Header{}},
		},
		{
			name:     "Header function - one, not found, default value",
			template: "Header('x-header-3', 'default value')",
			result:   `"default value"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{}},
		},
		// Body(name, default)
		{
			name:     "Body function - invalid content type",
			template: "Body()",
			err:      "cannot parse request body: unsupported content type \"\", supported types: application/json and application/x-www-form-urlencoded",
			reqCtx:   &recordctx.Context{Body: "{}"},
		},
		{
			name:     "Body function - JSON",
			template: "Body()",
			result:   `{"foo":"bar"}`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{"foo":"bar"}`},
		},
		{
			name:     "Body function - JSON, empty",
			template: "Body()",
			result:   "{}",
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{}`},
		},
		{
			name:     "Body function - JSON, invalid",
			template: "Body()",
			err:      `cannot parse request body: invalid JSON: invalid character '.' looking for beginning of object key string`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{...`},
		},
		{
			name:     "Body function - form data",
			template: "Body()",
			result:   `{"foo":"bar"}`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}, Body: `foo=bar`},
		},
		{
			name:     "Body function - form data, empty",
			template: "Body()",
			result:   "{}",
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}, Body: ``},
		},
		{
			name:     "Body function - JSON path",
			template: "Body('foo1.foo2')",
			result:   `"bar"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{"foo1":{"foo2":"bar"}}`},
		},
		{
			name:     "Body function - JSON path, default value",
			template: "Body('foo1.foo2', 'default value')",
			result:   `"bar"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{"foo1":{"foo2":"bar"}}`},
		},
		{
			name:     "Body function - JSON path, not found",
			template: "Body('foo1.foo2')",
			err:      "path \"foo1.foo2\" not found in the body",
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{}`},
		},
		{
			name:     "Body function - JSON path, default value",
			template: "Body('foo1.foo2', 'default value')",
			result:   `"default value"`,
			reqCtx:   &recordctx.Context{Headers: http.Header{"Content-Type": []string{"application/vnd.api+json"}}, Body: `{}`},
		},
	}

	pool := NewPool()

	for _, tc := range cases {
		vm := pool.Get()

		res, err := Evaluate(vm, tc.reqCtx, tc.template)
		if tc.err == "" {
			res = strings.TrimRight(res, "\n")
			assert.Equal(t, tc.result, res, tc.name)
			assert.NoError(t, err, tc.name)
		} else if assert.Error(t, err) {
			assert.Equal(t, tc.err, err.Error(), tc.name)
		}

		pool.Put(vm)
	}
}

func TestBufferJsonnet_InfiniteRecursion(t *testing.T) {
	t.Parallel()
	template := `
	local someFn(x) = x + someFn(x+1);
	{recursion: someFn(1)}
`
	pool := NewPool()
	vm := pool.Get()
	defer pool.Put(vm)

	_, err := Evaluate(vm, &recordctx.Context{}, template)
	assert.Error(t, err)
	assert.Equal(t, "max stack frames exceeded.", err.Error())
}

type testCase struct {
	name     string
	template string
	result   string
	reqCtx   recordctx.Context
	err      string
}
