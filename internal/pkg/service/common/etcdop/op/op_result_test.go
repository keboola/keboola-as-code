package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestResult(t *testing.T) {
	t.Parallel()

	expectedHeader := &etcdserverpb.ResponseHeader{}
	expectedResponse := newRawResponse(nil, nil).WithOpResponse((&etcd.GetResponse{Header: expectedHeader}).OpResponse())

	result := newResult[string](expectedResponse, nil)

	// Response
	assert.Same(t, expectedResponse, result.Response())

	// HeaderOrErr
	header, err := result.HeaderOrErr()
	assert.Same(t, expectedHeader, header)
	require.NoError(t, err)

	// ResultOrErr
	str, err := result.ResultOrErr()
	assert.Empty(t, str)
	require.NoError(t, err)

	// Result
	assert.Empty(t, result.Result())

	// Err/AddErr
	require.NoError(t, result.Err())
	assert.Same(t, expectedHeader, result.Header())
	result.AddErr(errors.New("error1"))
	result.AddErr(errors.New("error2"))
	if err := result.Err(); assert.Error(t, err) {
		assert.Equal(t, "- error1\n- error2", err.Error())
	}

	// Reset
	result.ResetErr()
	require.NoError(t, result.Err())
}
