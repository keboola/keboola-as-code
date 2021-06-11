package api

import (
	"context"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"keboola-as-code/src/http"
	"keboola-as-code/src/model/remote"
	"keboola-as-code/src/options"
	"keboola-as-code/src/tests"
	"keboola-as-code/src/utils"
	"testing"
	"time"
)

type StorageApi struct {
	apiHost    string
	apiHostUrl string
	client     *http.Client
	token      *remote.Token
}

func NewStorageApiFromOptions(options *options.Options, ctx context.Context, logger *zap.SugaredLogger) (*StorageApi, error) {
	if len(options.ApiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	if len(options.ApiToken) == 0 {
		panic(fmt.Errorf("api token is not set"))
	}

	storageApi := NewStorageApi(options.ApiHost, ctx, logger, options.VerboseApi)
	token, err := storageApi.GetToken(options.ApiToken)
	if err != nil {
		if v, ok := err.(http.ErrorWithResponse); ok && v.IsUnauthorized() {
			return nil, fmt.Errorf("the specified storage API token is not valid")
		} else {
			return nil, fmt.Errorf("token verification failed: %s", err)
		}
	}
	if !token.IsMaster {
		return nil, fmt.Errorf("required master token, but the given token is not master")
	}

	logger.Debugf("Storage API token is valid.")
	logger.Debugf(`Project id: "%d", project name: "%s".`, token.ProjectId(), token.ProjectName())
	return storageApi.WithToken(token), nil
}

func NewStorageApi(apiHost string, ctx context.Context, logger *zap.SugaredLogger, verbose bool) *StorageApi {
	apiHostUrl := "https://" + apiHost + "/v2/storage"
	client := http.NewHttpClient(ctx, logger, verbose).WithHostUrl(apiHostUrl)
	client.SetError(&Error{})
	return &StorageApi{client: client, apiHost: apiHost, apiHostUrl: apiHostUrl}
}

func (a *StorageApi) Host() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHost
}

func (a *StorageApi) HostUrl() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHostUrl
}

// Req creates request
func (a *StorageApi) Req(method string, url string) *resty.Request {
	return a.client.Req(method, url)
}

func (a *StorageApi) SetRetry(count int, waitTime time.Duration, maxWaitTime time.Duration) {
	a.client.SetRetry(count, waitTime, maxWaitTime)
}

// Methods for tests:

func TestStorageApi(t *testing.T) (*StorageApi, *utils.Writer) {
	return TestStorageApiWithHost(t, tests.TestApiHost())
}

func TestStorageApiWithHost(t *testing.T, apiHost string) (*StorageApi, *utils.Writer) {
	logger, logs := utils.NewDebugLogger()
	a := NewStorageApi(apiHost, context.Background(), logger, false)
	a.SetRetry(3, 1*time.Millisecond, 1*time.Millisecond)
	return a, logs
}

func TestStorageApiWithToken(t *testing.T) (*StorageApi, *utils.Writer) {
	a, logs := TestStorageApiWithHost(t, tests.TestApiHost())
	token, err := a.GetToken(tests.TestTokenMaster())
	assert.NoError(t, err)
	return a.WithToken(token), logs
}
