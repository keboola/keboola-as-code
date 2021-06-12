package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
)

func (a *StorageApi) DeleteConfiguration(componentId string, id string) *client.Response {
	return a.DeleteConfigurationReq(componentId, id).Send().Response()
}

func (a *StorageApi) DeleteConfigurationReq(componentId string, id string) *client.Request {
	return a.Request(resty.MethodDelete, fmt.Sprintf("components/%s/configs/%s", componentId, id))
}
