package runner

import (
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessPathReference(t *testing.T) {
	t.Parallel()

	resp := orderedmap.New()
	resp.Set("url", "https://stream.keboola.local/v1/sources/my-source/tasks/source.create/2023-02-14T12:49:07.026Z_M6C_a")
	requests := map[string]*APIRequest{
		"req1": {
			Definition: APIRequestDef{},
			Response:   resp,
		},
	}

	// No reference
	res, err := processPathReference("/foo/bar", requests)
	require.NoError(t, err)
	assert.Equal(t, "/foo/bar", res)

	// Correct reference
	res, err = processPathReference("<<req1:response.url>>", requests)
	require.NoError(t, err)
	assert.Equal(t, "/v1/sources/my-source/tasks/source.create/2023-02-14T12:49:07.026Z_M6C_a", res)

	// Incorrect referenced request name
	_, err = processPathReference("<<req2:response.url>>", requests)
	require.ErrorContains(t, err, "invalid request reference in the request path")

	// Incorrect reference path
	_, err = processPathReference("<<req1:response.invalid>>", requests)
	require.ErrorContains(t, err, `path "invalid" not found`)
}
