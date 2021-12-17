package version

import (
	"context"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
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
	assert.NotContains(t, logs.String(), `WARN`)
}

func TestCheckIfLatestVersionGreater(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.5`)
	assert.Nil(t, err)
	assert.NotContains(t, logs.String(), `WARN`)
}

func TestCheckIfLatestVersionLess(t *testing.T) {
	t.Parallel()
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.2`)
	assert.Nil(t, err)
	assert.Contains(t, logs.String(), `WARN  WARNING: A new version "v1.2.3" is available.`)
}

func createMockedChecker(t *testing.T) (*checker, *utils.Writer) {
	t.Helper()

	logger, logs := utils.NewDebugLogger()
	c := NewGitHubChecker(context.Background(), logger, env.Empty())
	resty := c.api.GetRestyClient()

	// Set short retry delay in tests
	resty.RetryWaitTime = 1 * time.Millisecond
	resty.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked resty transport
	httpTransport := httpmock.NewMockTransport()
	resty.GetClient().Transport = httpTransport

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

	return c, logs
}
