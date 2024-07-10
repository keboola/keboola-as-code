package column_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

func BenchmarkColumn_Template_Jsonnet(b *testing.B) {
	c := column.Template{Template: column.TemplateConfig{
		Language: column.TemplateLanguageJsonnet,
		Content:  "Body('key1[1].key3')",
	}}

	body := `{"key1":[{"key2":"val2"},{"key3":"val3"}]}`
	header := http.Header{"Content-Type": []string{"application/json"}}
	reqCtx := &recordctx.Context{Body: body, Headers: header}
	renderer := column.NewRenderer()

	for i := 0; i < b.N; i++ {
		val, err := renderer.CSVValue(c, reqCtx)
		assert.NoError(b, err)
		assert.Equal(b, `"val3"`, val)
	}
}

func BenchmarkColumn_UUID(b *testing.B) {
	c := column.UUID{}

	reqCtx := &recordctx.Context{}
	renderer := column.NewRenderer()

	for i := 0; i < b.N; i++ {
		val, err := renderer.CSVValue(c, reqCtx)
		assert.NoError(b, err)
		assert.Len(b, val, 36)
	}
}
