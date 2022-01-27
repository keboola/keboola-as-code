package storageapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type Api struct {
	apiHost    string
	apiHostUrl string
	client     *client.Client
	logger     log.Logger
	token      *model.Token
	components *model.ComponentsMap
}

func NewWithToken(ctx context.Context, logger log.Logger, host, tokenStr string, verbose bool) (*Api, error) {
	if len(host) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	if len(tokenStr) == 0 {
		panic(fmt.Errorf("api token is not set"))
	}

	storageApi := New(host, ctx, logger, verbose)
	token, err := storageApi.GetToken(tokenStr)
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

func New(apiHost string, ctx context.Context, logger log.Logger, verbose bool) *Api {
	apiHostUrl := "https://" + apiHost + "/v2/storage"
	c := client.NewClient(ctx, logger, verbose).WithHostUrl(apiHostUrl)
	c.SetError(&Error{})
	api := &Api{client: c, logger: logger, apiHost: apiHost, apiHostUrl: apiHostUrl}
	api.components = model.NewComponentsMap(api)
	return api
}

func (a *Api) Components() *model.ComponentsMap {
	return a.components
}

func (a *Api) Host() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHost
}

func (a *Api) HostUrl() string {
	if len(a.apiHost) == 0 {
		panic(fmt.Errorf("api host is not set"))
	}
	return a.apiHostUrl
}

func (a *Api) NewPool() *client.Pool {
	return a.client.NewPool(a.logger)
}

func (a *Api) NewRequest(method string, url string) *client.Request {
	return a.client.NewRequest(method, url)
}

func (a *Api) Send(request *client.Request) {
	a.client.Send(request)
}

func (a *Api) SetRetry(count int, waitTime time.Duration, maxWaitTime time.Duration) {
	a.client.SetRetry(count, waitTime, maxWaitTime)
}

func (a *Api) RestyClient() *resty.Client {
	return a.client.GetRestyClient()
}

func (a *Api) HttpClient() *http.Client {
	return a.client.GetRestyClient().GetClient()
}

func getChangedValues(all map[string]string, changedFields model.ChangedFields) map[string]string {
	// Filter
	data := map[string]string{}
	for key := range changedFields {
		if v, ok := all[key]; ok {
			data[key] = v
		} else {
			panic(fmt.Errorf(`changed field "%s" not found in API values`, key))
		}
	}
	return data
}
