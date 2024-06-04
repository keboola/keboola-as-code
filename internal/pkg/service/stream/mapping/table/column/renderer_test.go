package column_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

func TestRenderer_UUID(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.UUID{}

	val, err := renderer.CSVValue(c, &receivectx.Context{})
	require.NoError(t, err)
	id, err := uuid.FromString(val)
	require.NoError(t, err)
	assert.Equal(t, uuid.V7, id.Version())
}

func TestRenderer_DateTime(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Datetime{}

	now, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	val, err := renderer.CSVValue(c, &receivectx.Context{Now: now})
	require.NoError(t, err)
	assert.Equal(t, "2006-01-02T08:04:05.000Z", val)
}

func TestRenderer_IP(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.IP{}

	val, err := renderer.CSVValue(c, &receivectx.Context{IP: net.ParseIP("1.2.3.4")})
	require.NoError(t, err)
	assert.Equal(t, "1.2.3.4", val)
}

func TestRenderer_Body(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Body{}

	body := "a,b,c"
	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body})
	require.NoError(t, err)
	assert.Equal(t, "a,b,c", val)
}

func TestRenderer_Headers(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Headers{}

	header := http.Header{"Foo1": []string{"bar1"}, "Foo2": []string{"bar2", "bar3"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `{"Foo1":"bar1","Foo2":"bar2"}`, val)
}

func TestRenderer_Template_Json_Scalar(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1.key2')"}

	body := `{"key1":{"key2":"val2"},"key3":"val3"}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, "\"val2\"", val)
}

func TestRenderer_Template_Json_Object(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1')"}

	body := `{"key1":{"key2":"val2"},"key3":"val3"}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `{"key2":"val2"}`, val)
}

func TestRenderer_Template_Json_ArrayOfObjects(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1')"}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `[{"key2":"val2","key3":"val3"}]`, val)
}

func TestRenderer_Template_Json_ArrayIndex(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1[1].key3')"}

	body := `{"key1":[{"key2":"val2"},{"key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `"val3"`, val)
}

func TestRenderer_Template_Json_Full(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body()`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `{"key1":[{"key2":"val2","key3":"val3"}]}`, val)
}

func TestRenderer_Template_Json_UndefinedKey_Error(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body('key1.invalid')`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	_, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.Error(t, err)
	assert.Equal(t, `path "key1.invalid" not found in the body`, err.Error())
}

func TestRenderer_Template_Json_UndefinedKey_DefaultValue(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body('key1.invalid', 123)`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, "123", val)
}

func TestRenderer_Template_FormData_Full(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body()`}

	body := `key1=bar1&key2[]=bar2&key2[]=bar3`
	header := http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Body: body, Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `{"key1":"bar1","key2[]":["bar2","bar3"]}`, val)
}

func TestRenderer_Template_Headers(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Content-Encoding')`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	require.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestRenderer_Template_Headers_Case(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('CONTENT-ENCODING')`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	require.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestRenderer_Template_Headers_All(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header()`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `{"Content-Encoding":"gzip","Content-Type":"application/json"}`, val)
}

func TestRenderer_Template_Headers_UndefinedKey_Error(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Invalid-KEY')`}

	header := http.Header{"Content-Type": []string{"application/json"}}

	_, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	assert.ErrorContains(t, err, `header "Invalid-Key" not found`)
}

func TestRenderer_Template_Headers_UndefinedKey_DefaultValue(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Invalid-KEY', "abc")`}

	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := renderer.CSVValue(c, &receivectx.Context{Headers: header})
	require.NoError(t, err)
	assert.Equal(t, `"abc"`, val)
}

func TestRenderer_Template_InvalidLanguage(t *testing.T) {
	t.Parallel()

	renderer := column.NewRenderer()
	c := column.Template{Language: "invalid", Content: `Body("")`}

	_, err := renderer.CSVValue(c, &receivectx.Context{})
	assert.ErrorContains(t, err, `unsupported language "invalid", only "jsonnet" is supported`)
}
