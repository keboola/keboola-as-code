package encryptionapi

import (
	"context"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"

	"github.com/keboola/keboola-as-code/internal/pkg/http"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Api struct {
	hostUrl   string
	projectId int
	client    *http.Client
	logger    log.Logger
}

func New(ctx context.Context, logger log.Logger, hostUrl string, projectId int, verbose bool) *Api {
	c := http.NewClient(ctx, logger, verbose).WithBaseUrl(hostUrl)
	c.SetError(&Error{})
	api := &Api{projectId: projectId, client: c, logger: logger, hostUrl: hostUrl}
	return api
}

func (a *Api) NewPool() *http.Pool {
	return a.client.NewPool(a.logger)
}

func (a *Api) NewRequest(method string, url string) *http.Request {
	return a.client.NewRequest(method, url)
}

func (a *Api) CreateEncryptRequest(componentId model.ComponentId, data map[string]string) *http.Request {
	result := make(map[string]string)
	return a.
		client.NewRequest(resty.MethodPost, "encrypt").
		SetQueryParam("componentId", componentId.String()).
		SetQueryParam("projectId", cast.ToString(a.projectId)).
		SetJsonBody(data).
		SetResult(&result)
}
