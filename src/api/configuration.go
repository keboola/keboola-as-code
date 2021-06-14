package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
)

func (a *StorageApi) DeleteConfiguration(componentId string, id string) *client.Response {
	return a.DeleteConfigurationRequest(componentId, id).Send().Response()
}

func (a *StorageApi) DeleteConfigurationRequest(componentId string, id string) *client.Request {
	return a.Request(resty.MethodDelete, fmt.Sprintf("components/%s/configs/%s", componentId, id))
}
