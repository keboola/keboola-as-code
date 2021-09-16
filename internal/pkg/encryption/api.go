package encryption

import (
	"context"
	"fmt"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
)

type Api struct {
	hostUrl string
	client  *client.Client
	logger  *zap.SugaredLogger
}

// Error represents Encryption API error structure.
type Error struct {
	Message     string `json:"error"`
	ErrCode     int    `json:"code"`
	ExceptionId string `json:"exceptionId"`
}

func (e *Error) Error() string {
	msg := fmt.Sprintf(`"%v", errCode: "%v"`, e.Message, e.ErrCode)
	if len(e.ExceptionId) > 0 {
		msg += fmt.Sprintf(`, exceptionId: "%s"`, e.ExceptionId)
	}
	return msg
}

func NewEncryptionApi(hostUrl string, ctx context.Context, logger *zap.SugaredLogger, verbose bool) *Api {
	c := client.NewClient(ctx, logger, verbose).WithHostUrl(hostUrl)
	c.SetError(&Error{})
	api := &Api{client: c, logger: logger, hostUrl: hostUrl}
	return api
}

func (a *Api) NewPool() *client.Pool {
	return a.client.NewPool(a.logger)
}

func (a *Api) NewRequest(method string, url string) *client.Request {
	return a.client.NewRequest(method, url)
}

func (a *Api) CreateEncryptRequest(componentId string, projectId int, data map[string]string) *client.Request {
	result := make(map[string]string)
	request := a.
		client.NewRequest(resty.MethodPost, "encrypt").
		SetQueryParam("componentId", componentId).
		SetQueryParam("projectId", cast.ToString(projectId)).
		SetResult(&result)
	request.Request.SetBody(data)
	request.Request.SetHeader("Content-Type", "application/json")
	return request
}
