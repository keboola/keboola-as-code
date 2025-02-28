package version

import (
	"context"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestCheckIfLatestVersionDev(t *testing.T) {
	t.Parallel()
	c, _ := createMockedChecker(t)
	err := c.CheckIfLatest(t.Context(), build.DevVersionValue)
	require.Error(t, err)
	assert.Equal(t, `skipped, found dev build`, err.Error())
}

func TestCheckIfLatestVersionEqual(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(t.Context(), `v1.2.3`)
	require.NoError(t, err)
	assert.NotContains(t, logs.AllMessages(), `warn`)
}

func TestCheckIfLatestVersionGreater(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(t.Context(), `v1.2.5`)
	require.NoError(t, err)
	assert.NotContains(t, logs.AllMessages(), `warn`)
}

func TestCheckIfLatestVersionLess(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(t.Context(), `v1.2.2`)
	require.NoError(t, err)
	expected := `
{"level":"warn","message":"*******************************************************"}
{"level":"warn","message":"WARNING: A new version \"v1.2.3\" is available."}
{"level":"warn","message":"You are currently using version \"1.2.2\"."}
{"level":"warn","message":"Please update to get the latest features and bug fixes."}
{"level":"warn","message":"Read more: https://github.com/keboola/keboola-as-code/releases"}
{"level":"warn","message":"*******************************************************"}
{"level":"warn","message":""}
`
	logs.AssertJSONMessages(t, expected)
}

func createMockedChecker(t *testing.T) (*checker, log.DebugLogger) {
	t.Helper()

	// Mocked response
	body := `
[
  {
    "tag_name": "v1.2.4",
    "assets": []
  },
  {
    "tag_name": "v1.2.3",
    "assets": [
      {
         "id": 123
      }
    ]
  }
]
`
	bodyJSON := make([]any, 0)
	json.MustDecodeString(body, &bodyJSON)
	httpTransport := httpmock.NewMockTransport()
	httpTransport.RegisterResponder("GET", `https://api.github.com/repos/keboola/keboola-as-code/releases`, httpmock.NewJsonResponderOrPanic(200, bodyJSON))

	// Client with mocked http transport
	logger := log.NewDebugLogger()
	c := NewGitHubChecker(t.Context(), logger, false)
	c.client = c.client.WithTransport(httpTransport).WithRetry(client.TestingRetry())
	return c, logger
}
