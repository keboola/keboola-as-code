package column_test

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/receive/receivectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

func TestMappedColumns_Serde(t *testing.T) {
	t.Parallel()

	typed := column.Columns{
		column.ID{Name: "id", PrimaryKey: true},
		column.Datetime{Name: "datetime"},
		column.IP{Name: "ip"},
		column.Body{Name: "body"},
		column.Headers{Name: "headers"},
		column.Template{
			Name:     "template",
			Language: column.TemplateLanguageJsonnet,
			Content:  `Body('my.key')+':'+Body('my.value')'`,
		},
	}
	expectedJSON := `[{"type":"id","name":"id","primaryKey":true},{"type":"datetime","name":"datetime"},{"type":"ip","name":"ip"},{"type":"body","name":"body"},{"type":"headers","name":"headers"},{"type":"template","name":"template","language":"jsonnet","content":"Body('my.key')+':'+Body('my.value')'"}]`

	bytes, err := json.Marshal(&typed)
	assert.NoError(t, err)
	assert.Equal(t, expectedJSON, string(bytes))

	var output column.Columns
	err = json.Unmarshal(bytes, &output)
	assert.NoError(t, err)
	assert.Equal(t, typed, output)
}

func TestMappedColumns_Serde_UnknownType(t *testing.T) {
	t.Parallel()

	var output column.Columns
	err := json.Unmarshal([]byte(`[{"type":"unknown"}]`), &output)
	assert.Error(t, err, `invalid column type "unknown"`)
}

func TestColumn_ID(t *testing.T) {
	t.Parallel()

	c := column.ID{}

	val, err := c.CSVValue(&receivectx.Context{})
	assert.NoError(t, err)
	assert.Equal(t, column.IDPlaceholder, val)
}

func TestColumn_DateTime(t *testing.T) {
	t.Parallel()

	c := column.Datetime{}

	now, _ := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	val, err := c.CSVValue(&receivectx.Context{Now: now})
	assert.NoError(t, err)
	assert.Equal(t, "2006-01-02T08:04:05.000Z", val)
}

func TestColumn_IP(t *testing.T) {
	t.Parallel()

	c := column.IP{}

	val, err := c.CSVValue(&receivectx.Context{IP: net.ParseIP("1.2.3.4")})
	assert.NoError(t, err)
	assert.Equal(t, "1.2.3.4", val)
}

func TestColumn_Body(t *testing.T) {
	t.Parallel()

	c := column.Body{}

	body := "a,b,c"
	val, err := c.CSVValue(&receivectx.Context{Body: body})
	assert.NoError(t, err)
	assert.Equal(t, "a,b,c", val)
}

func TestColumn_Headers(t *testing.T) {
	t.Parallel()

	c := column.Headers{}

	header := http.Header{"Foo1": []string{"bar1"}, "Foo2": []string{"bar2", "bar3"}}

	val, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `{"Foo1":"bar1","Foo2":"bar2"}`, val)
}

func TestColumn_Template_Json_Scalar(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1.key2')"}

	body := `{"key1":{"key2":"val2"},"key3":"val3"}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"val2\"", val)
}

func TestColumn_Template_Json_Object(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1')"}

	body := `{"key1":{"key2":"val2"},"key3":"val3"}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `{"key2":"val2"}`, val)
}

func TestColumn_Template_Json_ArrayOfObjects(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1')"}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `[{"key2":"val2","key3":"val3"}]`, val)
}

func TestColumn_Template_Json_ArrayIndex(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1[1].key3')"}

	body := `{"key1":[{"key2":"val2"},{"key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `"val3"`, val)
}

func TestColumn_Template_Json_Full(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body()`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `{"key1":[{"key2":"val2","key3":"val3"}]}`, val)
}

func TestColumn_Template_Json_UndefinedKey_Error(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body('key1.invalid')`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	_, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.Error(t, err)
	assert.Equal(t, `path "key1.invalid" not found in the body`, err.Error())
}

func TestColumn_Template_Json_UndefinedKey_DefaultValue(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body('key1.invalid', 123)`}

	body := `{"key1":[{"key2":"val2","key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, "123", val)
}

func TestColumn_Template_FormData_Full(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body()`}

	body := `key1=bar1&key2[]=bar2&key2[]=bar3`
	header := http.Header{"Content-Type": []string{"application/x-www-form-urlencoded"}}

	val, err := c.CSVValue(&receivectx.Context{Body: body, Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `{"key1":"bar1","key2[]":["bar2","bar3"]}`, val)
}

func TestColumn_Template_Headers(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Content-Encoding')`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestColumn_Template_Headers_Case(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('CONTENT-ENCODING')`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestColumn_Template_Headers_All(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header()`}

	header := http.Header{"Content-Type": []string{"application/json"}, "Content-Encoding": []string{"gzip"}}

	val, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `{"Content-Encoding":"gzip","Content-Type":"application/json"}`, val)
}

func TestColumn_Template_Headers_UndefinedKey_Error(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Invalid-KEY')`}

	header := http.Header{"Content-Type": []string{"application/json"}}

	_, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.ErrorContains(t, err, `header "Invalid-Key" not found`)
}

func TestColumn_Template_Headers_UndefinedKey_DefaultValue(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Invalid-KEY', "abc")`}

	header := http.Header{"Content-Type": []string{"application/json"}}

	val, err := c.CSVValue(&receivectx.Context{Headers: header})
	assert.NoError(t, err)
	assert.Equal(t, `"abc"`, val)
}

func TestColumn_Template_InvalidLanguage(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: "invalid", Content: `Body("")`}

	_, err := c.CSVValue(&receivectx.Context{})
	assert.ErrorContains(t, err, `unsupported language "invalid", only "jsonnet" is supported`)
}
