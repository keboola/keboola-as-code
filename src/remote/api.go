package remote

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"keboola-as-code/src/client"
	"keboola-as-code/src/components"
	"keboola-as-code/src/model"
	"keboola-as-code/src/options"
	"keboola-as-code/src/utils"
)

type StorageApi struct {
	apiHost    string
	apiHostUrl string
	client     *client.Client
	logger     *zap.SugaredLogger
	token      *model.Token
	components *components.Provider
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
		var errWithResponse client.ErrorWithResponse
		if errors.As(err, &errWithResponse) && errWithResponse.IsUnauthorized() {
			return nil, fmt.Errorf("the specified storage API token is not valid")
		} else {
			return nil, utils.PrefixError("token verification failed", err)
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
	c := client.NewClient(ctx, logger, verbose).WithHostUrl(apiHostUrl)
	c.SetError(&Error{})
	api := &StorageApi{client: c, logger: logger, apiHost: apiHost, apiHostUrl: apiHostUrl}
	api.components = components.NewProvider(api)
	return api
}

func (a *StorageApi) Components() *components.Provider {
	return a.components
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

func (a *StorageApi) NewPool() *client.Pool {
	return a.client.NewPool(a.logger)
}

func (a *StorageApi) NewRequest(method string, url string) *client.Request {
	return a.client.NewRequest(method, url)
}

func (a *StorageApi) Send(request *client.Request) {
	a.client.Send(request)
}

func (a *StorageApi) SetRetry(count int, waitTime time.Duration, maxWaitTime time.Duration) {
	a.client.SetRetry(count, waitTime, maxWaitTime)
}

func getChangedValues(all map[string]string, changed []string) map[string]string {
	if len(changed) == 0 {
		return all
	}

	// Filter
	data := map[string]string{}
	for _, key := range changed {
		if v, ok := all[key]; ok {
			data[key] = v
		} else {
			panic(fmt.Errorf(`key "%s" cannot be updated`, key))
		}
	}
	return data
}
