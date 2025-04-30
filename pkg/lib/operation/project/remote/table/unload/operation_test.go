package unload

import (
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseUnloadFormat(t *testing.T) {
	t.Parallel()

	_, err := ParseFormat("invalid")
	require.ErrorContains(t, err, `invalid format "invalid"`)

	v, err := ParseFormat("csv")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("Csv")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("CSV")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatCSV, v)
	v, err = ParseFormat("json")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)
	v, err = ParseFormat("Json")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)
	v, err = ParseFormat("JSON")
	require.NoError(t, err)
	assert.Equal(t, keboola.UnloadFormatJSON, v)
}
