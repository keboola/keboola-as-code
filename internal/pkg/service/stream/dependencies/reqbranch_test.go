package dependencies

import (
	"context"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestBranchRequestScope_DefaultBranch_String(t *testing.T) {
	t.Parallel()

	branchInput := key.BranchIDOrDefault("default")

	d, mock := NewMockedProjectRequestScope(t)
	client := mock.EtcdClient()

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mock.StorageAPIToken().Token)
	ctx := context.WithValue(mock.TestContext(), KeboolaProjectAPICtxKey, api)

	// Mock the Storage API call
	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet,
		"https://connection.keboola.local/v2/storage/dev-branches",
		httpmock.NewJsonResponderOrPanic(http.StatusOK,
			[]keboola.Branch{
				{
					BranchKey: keboola.BranchKey{ID: 456},
					IsDefault: true,
					Name:      "default",
				},
			}),
	)

	// There is no branch in DB
	etcdhelper.AssertKVsString(t, client, ``)

	// The first attempt is successful, the branch is loaded from the API
	transport.ZeroCallCounters()
	branchReqScp, err := newBranchRequestScope(ctx, d, branchInput)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://connection.keboola.local/v2/storage/dev-branches"])
	assert.Equal(t, keboola.BranchID(456), branchReqScp.Branch().BranchID)
	assert.True(t, branchReqScp.Branch().IsDefault)

	// Branch is create also in the database
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/12345/456
-----
{
  "projectId": 12345,
  "branchId": 456,
  "created": {
    "at": "%s",
    "by": {
      "type": "user",
      "tokenId": "%s"
    }
  },
  "isDefault": true
}
>>>>>
`)

	// The second attempt is successful, the branch is loaded from the database
	transport.ZeroCallCounters()
	branchReqScp, err = newBranchRequestScope(ctx, d, branchInput)
	require.NoError(t, err)
	assert.Equal(t, keboola.BranchID(456), branchReqScp.Branch().BranchID)
	assert.True(t, branchReqScp.Branch().IsDefault)
	assert.Equal(t, 0, transport.GetTotalCallCount())
	assert.Equal(t, 0, transport.GetCallCountInfo()["GET https://connection.keboola.local/v2/storage/dev-branches"])
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/12345/456
-----
{
  "projectId": 12345,
  "branchId": 456,
  "created": {
    "at": "%s",
    "by": {
      "type": "user",
      "tokenId": "%s"
    }
  },
  "isDefault": true
}
>>>>>
`)
}

func TestBranchRequestScope_DefaultBranch_Int(t *testing.T) {
	t.Parallel()

	branchInput := key.BranchIDOrDefault("456")

	d, mock := NewMockedProjectRequestScope(t)
	client := mock.EtcdClient()

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mock.StorageAPIToken().Token)
	ctx := context.WithValue(mock.TestContext(), KeboolaProjectAPICtxKey, api)

	// Mock the Storage API call
	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet,
		"https://connection.keboola.local/v2/storage/dev-branches/456",
		httpmock.NewJsonResponderOrPanic(http.StatusOK,
			keboola.Branch{
				BranchKey: keboola.BranchKey{ID: 456},
				IsDefault: true,
				Name:      "default",
			}),
	)

	// There is no branch in DB
	etcdhelper.AssertKVsString(t, client, ``)

	// The first attempt is successful, the branch is loaded from the API
	transport.ZeroCallCounters()
	branchReqScp, err := newBranchRequestScope(ctx, d, branchInput)
	require.NoError(t, err)
	assert.Equal(t, 1, transport.GetTotalCallCount())
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://connection.keboola.local/v2/storage/dev-branches/456"])
	assert.Equal(t, keboola.BranchID(456), branchReqScp.Branch().BranchID)
	assert.True(t, branchReqScp.Branch().IsDefault)

	// Branch is create also in the database
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/12345/456
-----
{
  "projectId": 12345,
  "branchId": 456,
  "created": {
    "at": "%s",
    "by": {
      "type": "user",
      "tokenId": "%s"
    }
  },
  "isDefault": true
}
>>>>>
`)

	// The second attempt is successful, the branch is loaded from the database
	transport.ZeroCallCounters()
	branchReqScp, err = newBranchRequestScope(ctx, d, branchInput)
	require.NoError(t, err)
	assert.Equal(t, keboola.BranchID(456), branchReqScp.Branch().BranchID)
	assert.True(t, branchReqScp.Branch().IsDefault)
	assert.Equal(t, 0, transport.GetTotalCallCount())
	assert.Equal(t, 0, transport.GetCallCountInfo()["GET https://connection.keboola.local/v2/storage/dev-branches/456"])
	etcdhelper.AssertKVsString(t, client, `
<<<<<
definition/branch/active/12345/456
-----
{
  "projectId": 12345,
  "branchId": 456,
  "created": {
    "at": "%s",
    "by": {
      "type": "user",
      "tokenId": "%s"
    }
  },
  "isDefault": true
}
>>>>>
`)
}

func TestBranchRequestScope_NotDefaultBranch(t *testing.T) {
	t.Parallel()

	branchInput := key.BranchIDOrDefault("456")

	d, mock := NewMockedProjectRequestScope(t)

	// Simulate that the operation is running in an API request authorized by a token
	api := d.KeboolaPublicAPI().WithToken(mock.StorageAPIToken().Token)
	ctx := context.WithValue(mock.TestContext(), KeboolaProjectAPICtxKey, api)

	// Mock the Storage API call
	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet,
		"https://connection.keboola.local/v2/storage/dev-branches/456",
		httpmock.NewJsonResponderOrPanic(http.StatusOK,
			keboola.Branch{
				BranchKey: keboola.BranchKey{ID: 456},
				IsDefault: false,
				Name:      "dev",
			}),
	)

	// Currently, only the default branch can be used
	transport.ZeroCallCounters()
	_, err := newBranchRequestScope(ctx, d, branchInput)
	if assert.Error(t, err) {
		assert.Equal(t, "currently only default branch is supported, branch \"456\" is not default", err.Error())
	}
	assert.Equal(t, 1, transport.GetTotalCallCount())
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://connection.keboola.local/v2/storage/dev-branches/456"])
}
