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
			DataType:               "STRING",
		},
	}
	untyped := `[{"type":"id"},{"type":"datetime"},{"type":"ip"},{"type":"body"},{"type":"headers"},{"type":"template","language":"jsonnet","undefinedValueStrategy":"null","content":"body.my.key+\":\"+body.my.value","dataType":"STRING"}]`

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

func TestColumn_Template_Body(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: "Body('key1.key2')"}

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
	assert.Equal(t, "\"val2\"\n", val)
}

func TestColumn_Template_Headers(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Headers('Content-Encoding')`}

	body := orderedmap.New()

	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Content-Encoding", "gzip")

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "\"gzip\"\n", val)
}

func TestColumn_Template_UndefinedKeyErr(t *testing.T) {
	t.Parallel()

	c := column.Template{Language: column.TemplateLanguageJsonnet, Content: `Headers('Invalid-Key')`}

	body := orderedmap.New()
	header := http.Header{}

	_, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.Error(t, err, "Field does not exist: Invalid-Key")
}

func TestColumn_Template_UndefinedKeyNil(t *testing.T) {
	t.Parallel()

	c := column.Template{
		Language:               column.TemplateLanguageJsonnet,
		Content:                `Headers('Invalid-Key')`,
		UndefinedValueStrategy: column.UndefinedValueStrategyNull,
	}

	body := orderedmap.New()
	header := http.Header{}

	val, err := c.CsvValue(column.ImportCtx{Body: body, Header: header})
	assert.NoError(t, err)
	assert.Equal(t, "null\n", val)
}
