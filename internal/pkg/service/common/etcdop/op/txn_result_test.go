package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestTxnResult(t *testing.T) {
	t.Parallel()

	expectedHeader := &etcdserverpb.ResponseHeader{}
	expectedResponse := newRawResponse(nil, nil).WithOpResponse((&etcd.TxnResponse{Succeeded: true, Header: expectedHeader}).OpResponse())

	str1 := "foo"
	result := newTxnResult(newResultBase(expectedResponse), &str1)

	// Response
	assert.Same(t, expectedResponse, result.Response())

	// HeaderOrErr
	header, err := result.HeaderOrErr()
	assert.Same(t, expectedHeader, header)
	require.NoError(t, err)

	// ResultOrErr
	resultValue, err := result.ResultOrErr()
	assert.Equal(t, "foo", resultValue)
	require.NoError(t, err)

	// Result
	assert.Equal(t, "foo", result.Result())

	// Err/AddErr
	require.NoError(t, result.Err())
	assert.Same(t, expectedHeader, result.Header())
	result.AddErr(errors.New("error1"))
	result.AddErr(errors.New("error2"))
	if err := result.Err(); assert.Error(t, err) {
		assert.Equal(t, "- error1\n- error2", err.Error())
	}

	// Succeeded
	assert.True(t, result.Succeeded())

	// Reset
	result.ResetErr()
	assert.NoError(t, result.Err())
}
