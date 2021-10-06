package remote

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestStorageApi(t *testing.T) (*StorageApi, *utils.Writer) {
	return TestStorageApiWithHost(t, testhelper.TestApiHost())
}

func TestMockedStorageApi(t *testing.T) (*StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	if testhelper.TestIsVerbose() {
		logs.ConnectTo(os.Stdout)
	}
	// Set short retry delay in tests
	api := NewStorageApi("connection.keboola.com", context.Background(), logger, false)
	api.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	api = api.WithToken(&model.Token{Owner: model.TokenOwner{Id: 12345}})

	// Mocked resty transport
	httpmock.Activate()
	httpmock.ActivateNonDefault(api.client.GetRestyClient().GetClient())
	t.Cleanup(func() {
		httpmock.DeactivateAndReset()
	})

	return api, logs
}

func TestStorageApiWithHost(t *testing.T, apiHost string) (*StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	if testhelper.TestIsVerbose() {
		logs.ConnectTo(os.Stdout)
	}
	a := NewStorageApi(apiHost, context.Background(), logger, false)
	a.SetRetry(3, 100*time.Millisecond, 100*time.Millisecond)
	return a, logs
}

func TestStorageApiWithToken(t *testing.T) (*StorageApi, *utils.Writer) {
	a, logs := TestStorageApiWithHost(t, testhelper.TestApiHost())
	token, err := a.GetToken(testhelper.TestToken())
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return a.WithToken(token), logs
}
