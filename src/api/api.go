package api

import (
	"context"
	"fmt"
	"go.uber.org/zap"
	"keboola-as-code/src/model"
	"keboola-as-code/src/options"
)

type StorageApi struct {
	http       *Client
	apiHost    string
	apiHostUrl string
	token      *model.Token
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
		if v, ok := err.(ErrorWithResponse); ok && v.IsUnauthorized() {
			return nil, fmt.Errorf("the specified storage API token is not valid")
		} else {
			return nil, fmt.Errorf("token verification failed: %s", err)
		}
	}

	logger.Debugf("Storage API token is valid.")
	logger.Debugf(`Project id: "%d", project name: "%s".`, token.ProjectId(), token.ProjectName())
	return storageApi.WithToken(token), nil
}

func NewStorageApi(apiHost string, ctx context.Context, logger *zap.SugaredLogger, verbose bool) *StorageApi {
	apiHostUrl := "https://" + apiHost + "/v2/storage/"
	http := NewHttpClient(ctx, logger, verbose).WithHostUrl(apiHostUrl)
	http.resty.SetError(&Error{})
	return &StorageApi{http: http, apiHost: apiHost, apiHostUrl: apiHostUrl}
}

func (a *StorageApi) ApiHost() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHost
}

func (a *StorageApi) ApiHostUrl() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHostUrl
}
