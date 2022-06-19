package version

import (
	"context"
	"strings"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

func TestCheckIfLatestVersionDev(t *testing.T) {
	t.Parallel()
	c, _ := createMockedChecker(t)
	err := c.CheckIfLatest(build.DevVersionValue)
	assert.NotNil(t, err)
	assert.Equal(t, `skipped, found dev build`, err.Error())
}

func TestCheckIfLatestVersionEqual(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.3`)
	assert.Nil(t, err)
	assert.NotContains(t, logs.AllMessages(), `WARN`)
}

func TestCheckIfLatestVersionGreater(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.5`)
	assert.Nil(t, err)
	assert.NotContains(t, logs.AllMessages(), `WARN`)
}

func TestCheckIfLatestVersionLess(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.2`)
	assert.Nil(t, err)
	expected := `
WARN  *******************************************************
WARN  WARNING: A new version "v1.2.3" is available.
WARN  You are currently using version "1.2.2".
WARN  Please update to get the latest features and bug fixes.
WARN  Read more: https://github.com/keboola/keboola-as-code/releases
WARN  *******************************************************
WARN
`
	assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(logs.WarnMessages()))
}

func createMockedChecker(t *testing.T) (*checker, log.DebugLogger) {
	t.Helper()

	// Client with mocked http transport
	logger := log.NewDebugLogger()
	c := NewGitHubChecker(context.Background(), logger, env.Empty())
	httpTransport := httpmock.NewMockTransport()
	c.client = c.client.WithTransport(httpTransport).WithRetry(client.TestingRetry())

	// Mocked body
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
	// Mocked response
	bodyJson := make([]interface{}, 0)
	json.MustDecodeString(body, &bodyJson)
	responder, err := httpmock.NewJsonResponder(200, bodyJson)
	assert.NoError(t, err)
	httpTransport.RegisterResponder("GET", `=~.+repos/keboola/keboola-as-code/releases.+`, responder)

	return c, logger
}
