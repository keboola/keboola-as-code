package column_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
)

func TestMappedColumns_Serde(t *testing.T) {
	t.Parallel()

	typed := column.Columns{
		column.UUID{Name: "id", PrimaryKey: true},
		column.Datetime{Name: "datetime"},
		column.IP{Name: "ip"},
		column.Body{Name: "body"},
		column.Headers{Name: "headers"},
		column.Template{
			Name: "template",
			Template: column.TemplateConfig{
				Language: column.TemplateLanguageJsonnet,
				Content:  `Body('my.key')+':'+Body('my.value')'`,
			},
		},
	}

	expectedJSON := `[{"type":"uuid","name":"id","primaryKey":true},{"type":"datetime","name":"datetime"},{"type":"ip","name":"ip"},{"type":"body","name":"body"},{"type":"headers","name":"headers"},{"type":"template","name":"template","template":{"language":"jsonnet","content":"Body('my.key')+':'+Body('my.value')'"}}]`

	bytes, err := json.Marshal(&typed)
	require.NoError(t, err)
	assert.JSONEq(t, expectedJSON, string(bytes))

	var output column.Columns
	err = json.Unmarshal(bytes, &output)
	require.NoError(t, err)
	assert.Equal(t, typed, output)
}

func TestMappedColumns_Serde_UnknownType(t *testing.T) {
	t.Parallel()

	var output column.Columns
	err := json.Unmarshal([]byte(`[{"type":"unknown"}]`), &output)
	assert.Error(t, err, `invalid column type "unknown"`)
}
