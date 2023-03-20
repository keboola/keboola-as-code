package unload

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

func TestParseUnloadFormat(t *testing.T) {
	t.Parallel()

	_, err := ParseFormat("invalid")
	assert.ErrorContains(t, err, `invalid format "invalid"`)

	v, err := ParseFormat("csv")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("Csv")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("CSV")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("json")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)
	v, err = ParseFormat("Json")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)
	v, err = ParseFormat("JSON")
	assert.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)

}
