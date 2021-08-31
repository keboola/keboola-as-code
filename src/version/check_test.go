package version

import (
	"context"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/build"
	"keboola-as-code/src/json"
	"keboola-as-code/src/utils"
)

func TestCheckIfLatestVersionDev(t *testing.T) {
	c, _ := createMockedChecker(t)
	err := c.CheckIfLatest(build.DevVersionValue)
	assert.NotNil(t, err)
	assert.Equal(t, `skipped, found dev build`, err.Error())
}

func TestCheckIfLatestVersionOk(t *testing.T) {
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.3`)
	assert.Nil(t, err)
	assert.NotContains(t, logs.String(), `WARN`)
}

func TestCheckIfLatestVersionUpdate(t *testing.T) {
	c, logs := createMockedChecker(t)
	err := c.CheckIfLatest(`v1.2.2`)
	assert.Nil(t, err)
	assert.Contains(t, logs.String(), `WARN  WARNING: A new version "v1.2.3" is available.`)
}

func createMockedChecker(t *testing.T) (*checker, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	c := NewChecker(context.Background(), logger)
	resty := c.api.GetRestyClient()

	// Set short retry delay in tests
	resty.RetryWaitTime = 1 * time.Millisecond
	resty.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked resty transport
	httpmock.Activate()
	httpmock.ActivateNonDefault(resty.GetClient())
	t.Cleanup(func() {
		httpmock.DeactivateAndReset()
	})

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
	httpmock.RegisterResponder("GET", `=~.+repos/keboola/keboola-as-code/releases.+`, responder)

	return c, logs
}
