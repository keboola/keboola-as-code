package column_test

import (
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
)

func TestMappedColumns(t *testing.T) {
	t.Parallel()

	typed := column.Columns{
		column.ID{},
		column.Datetime{},
		column.IP{},
		column.Body{},
		column.Header{},
		column.Template{
			Language:               "jsonnet",
			UndefinedValueStrategy: "null",
			Content:                `body.my.key+":"+body.my.value`,
		},
	}
	untyped := `[{"type":"id"},{"type":"datetime"},{"type":"ip"},{"type":"body"},{"type":"headers"},{"type":"template","language":"jsonnet","undefinedValueStrategy":"null","content":"body.my.key+\":\"+body.my.value"}]`

	bytes, err := json.Marshal(&typed)
	assert.NoError(t, err)
	assert.Equal(t, untyped, string(bytes))

	var output column.Columns
	err = json.Unmarshal(bytes, &output)
	assert.NoError(t, err)
	assert.Equal(t, typed, output)
}

func TestMappedColumns_Error(t *testing.T) {
	t.Parallel()

	// Unmarshal unknown type
	var output column.Columns
	err := json.Unmarshal([]byte(`[{"type":"unknown"}]`), &output)
	assert.Error(t, err, `invalid column type name "unknown"`)
}

func TestColumn_ID(t *testing.T) {
	t.Parallel()

	c := column.ID{}

	val, err := c.CsvValue(column.ImportCtx{})
	assert.NoError(t, err)
	assert.Equal(t, column.IDPlaceholder, val)
}

func TestColumn_DateTime(t *testing.T) {
	t.Parallel()

	c := column.Datetime{}

	tm := time.Now()
	val, err := c.CsvValue(column.ImportCtx{DateTime: tm})
	assert.NoError(t, err)
	assert.Equal(t, tm.Format(time.RFC3339), val)
}

func TestColumn_IP(t *testing.T) {
	t.Parallel()

	c := column.IP{}

	val, err := c.CsvValue(column.ImportCtx{IP: net.ParseIP("1.2.3.4")})
	assert.NoError(t, err)
	assert.Equal(t, "1.2.3.4", val)
}

func TestColumn_Body(t *testing.T) {
	t.Parallel()

	c := column.Body{}

	body := orderedmap.New()
	body.Set("one", "two")
	body.Set("three", "four")
	val, err := c.CsvValue(column.ImportCtx{Body: body})
	assert.NoError(t, err)
	bodyMarshalled, err := json.Marshal(body)
	assert.NoError(t, err)
	assert.Equal(t, string(bodyMarshalled), val)
}

func TestColumn_Header(t *testing.T) {
	t.Parallel()

	c := column.Header{}

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")
	val, err := c.CsvValue(column.ImportCtx{Header: header})
	assert.NoError(t, err)
	headerMarshalled, err := json.Marshal(header)
	assert.NoError(t, err)
	assert.Equal(t, string(headerMarshalled), val)
}

func TestColumn_Template_Body_Scalar(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "BodyPath('key1.key2')"}

	val1 := orderedmap.New()
	val1.Set("key2", "val2")
	body := orderedmap.New()
	body.Set("key1", val1)
	body.Set("key3", "val3")

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"val2\"", val)
}

func TestColumn_Template_Body_Object(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "BodyPath('key1')"}

	val1 := orderedmap.New()
	val1.Set("key2", "val2")
	body := orderedmap.New()
	body.Set("key1", val1)
	body.Set("key3", "val3")

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"key2\": \"val2\"\n}", val)
}

func TestColumn_Template_Body_ArrayOfObjects(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "BodyPath('key1')"}

	val2 := orderedmap.New()
	val2.Set("key2", "val2")
	val3 := orderedmap.New()
	val3.Set("key3", "val3")
	body := orderedmap.New()
	body.Set("key1", []any{val2, val3})

	header := http.Header{}

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "[\n  {\n    \"key2\": \"val2\"\n  },\n  {\n    \"key3\": \"val3\"\n  }\n]", val)
}

func TestColumn_Template_Body_ArrayIndex(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "BodyPath('key1')[1]"}

	body := orderedmap.New()
	body.Set("key1", []any{"val2", "val3"})

	header := http.Header{}

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"val3\"", val)
}

func TestColumn_Template_Body_Full(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Body()`}

	body := orderedmap.New()
	body.Set("key1", []any{"val2", "val3"})

	header := http.Header{}

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"key1\": [\n    \"val2\",\n    \"val3\"\n  ]\n}", val)
}

func TestColumn_Template_Body_UndefinedKeyErr(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `BodyPath('key1.invalid')`}

	val1 := orderedmap.New()
	val1.Set("key2", "val2")
	body := orderedmap.New()
	body.Set("key1", val1)
	body.Set("key3", "val3")
	header := http.Header{}

	_, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.ErrorContains(t, err, `path "key1.invalid" not found in the body`)
}

func TestColumn_Template_Headers(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Content-Encoding')`}

	body := orderedmap.New()

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestColumn_Template_Headers_Case(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('CONTENT-ENCODING')`}

	body := orderedmap.New()

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"gzip\"", val)
}

func TestColumn_Template_Headers_All(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Headers()`}

	body := orderedmap.New()

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "{\n  \"Content-Encoding\": \"gzip\",\n  \"Content-Type\": \"application/json\"\n}", val)
}

func TestColumn_Template_Headers_UndefinedKeyErr(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Header('Invalid-KEY')`}

	body := orderedmap.New()
	header := http.Header{}

	_, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.ErrorContains(t, err, `header "Invalid-Key" not found`)
}

func TestColumn_Template_UndefinedKeyNil(t *testing.T) {
	t.Parallel()

	c := column.Template{
		Language:               column.TemplateLanguageJsonnet,
		Content:                `Header('Invalid-Key')`,
		UndefinedValueStrategy: column.UndefinedValueStrategyNull,
	}

	body := orderedmap.New()
	header := http.Header{}

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "null", val)
}

func TestColumn_Template_InvalidLanguage(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: "invalid", Content: `Body("")`}

	body := orderedmap.New()
	header := http.Header{}

	_, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.ErrorContains(t, err, `unsupported language "invalid", use jsonnet instead`)
}
