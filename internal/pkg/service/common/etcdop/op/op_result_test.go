package op

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestResult(t *testing.T) {
	t.Parallel()

	expectedHeader := &etcdserverpb.ResponseHeader{}
	expectedResponse := &RawResponse{
		OpResponse: (&clientv3.GetResponse{Header: expectedHeader}).OpResponse(),
	}

	result := newResult[string](expectedResponse, nil)

	// Response
	assert.Same(t, expectedResponse, result.Response())

	// HeaderOrErr
	header, err := result.HeaderOrErr()
	assert.Same(t, expectedHeader, header)
	require.NoError(t, err)

	// ResultOrErr
	str, err := result.ResultOrErr()
	assert.Equal(t, "", str)
	require.NoError(t, err)

	// Result/SetResult
	assert.Empty(t, result.Result())
	str2 := "foo"
	result.SetResult(&str2)
	assert.Equal(t, "foo", result.Result())

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
