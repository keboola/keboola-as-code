package encryption

import (
	"context"
	"fmt"
	"keboola-as-code/src/client"
	"keboola-as-code/src/remote"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"
)

type Api struct {
	apiHostUrl string
	client     *client.Client
	logger     *zap.SugaredLogger
}

// Error represents Encryption API error structure
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

func getEncryptionApiHost(connectionApiHost string, ctx context.Context, logger *zap.SugaredLogger) string {
	storageApi := remote.NewStorageApi(connectionApiHost, ctx, logger, false)
	services, err := storageApi.GetServices()
	if err != nil {
		panic(fmt.Errorf("failed to retrieve services from Storage API: \"%s\"", err))
	}

	for _, object := range services {
		service := object.(map[string]interface{})
		if service["id"] == "encryption" {
			apiHost := service["url"]
			return apiHost.(string)
		}
	}
	panic(fmt.Errorf("encryption API not found in services from Storage API: \"%s\"", services))
}

func NewEncryptionApi(connectionApiHost string, ctx context.Context, logger *zap.SugaredLogger, verbose bool) *Api {

	apiHostUrl := getEncryptionApiHost(connectionApiHost, ctx, logger)
	c := client.NewClient(ctx, logger, verbose).WithHostUrl(apiHostUrl)
	c.SetError(&Error{})
	api := &Api{client: c, logger: logger, apiHostUrl: apiHostUrl}
	return api
}

func (a *Api) NewRequest(method string, url string) *client.Request {
	return a.client.NewRequest(method, url)
}

func (a *Api) createRequest(componentId string, projectId string, requestBody map[string]string) (*client.Request, error) {
	// Create request
	result := make(map[string]string)
	request := a.
		client.NewRequest(resty.MethodPost, "encrypt").
		SetQueryParam("componentId", componentId).
		SetQueryParam("projectId", projectId).
		SetResult(&result)
	request.Request.SetBody(requestBody)
	request.Request.SetHeader("Content-Type", "application/json")

	return request, nil
}

func (a *Api) EncryptMapValues(componentId string, projectId string, mapValues map[string]string) (map[string]string, error) {
	request, err := a.createRequest(componentId, projectId, mapValues)
	if err != nil {
		return nil, err
	}
	response := request.Send().Response
	if response.HasResult() {
		return *response.Result().(*map[string]string), nil
	}
	return nil, response.Err()
}
