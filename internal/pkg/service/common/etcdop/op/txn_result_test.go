package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestTxnResult(t *testing.T) {
	t.Parallel()

	expectedHeader := &etcdserverpb.ResponseHeader{}
	expectedResponse := &RawResponse{
		OpResponse: (&clientv3.GetResponse{Header: expectedHeader}).OpResponse(),
	}

	str1 := "foo"
	result := newTxnResult(expectedResponse, &str1)

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

	// Result/SetResult
	assert.Equal(t, "foo", result.Result())
	str2 := "bar"
	result.SetResult(&str2)
	assert.Equal(t, "bar", result.Result())

	// SubResults/SetSubResult/AddSubResult
	assert.Empty(t, result.SubResults())
	result.SetSubResults([]any{"foo", "bar"})
	assert.Equal(t, []any{"foo", "bar"}, result.SubResults())
	result.AddSubResult("baz")
	assert.Equal(t, []any{"foo", "bar", "baz"}, result.SubResults())

	// Err/AddErr
	require.NoError(t, result.Err())
	assert.Same(t, expectedHeader, result.Header())
	result.AddErr(errors.New("error1"))
	result.AddErr(errors.New("error2"))
	if err := result.Err(); assert.Error(t, err) {
		assert.Equal(t, "- error1\n- error2", err.Error())
	}

	// Succeeded - false
	assert.False(t, result.Succeeded())

	// Reset
	result.ResetErr()
	require.NoError(t, result.Err())

	// Succeeded - true
	result.succeeded = true
	assert.True(t, result.Succeeded())
}
