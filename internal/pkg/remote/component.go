package remote

import (
	"github.com/go-resty/resty/v2"

	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func (a *StorageApi) GetComponent(componentId string) (*model.Component, error) {
	response := a.GetComponentRequest(componentId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Component), nil
	}
	return nil, response.Err()
}

// GetComponentRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/get-component/get-component
func (a *StorageApi) GetComponentRequest(componentId string) *client.Request {
	component := &model.Component{}
	component.Id = componentId
	return a.
		NewRequest(resty.MethodGet, "components/{componentId}").
		SetPathParam("componentId", componentId).
		SetResult(component).
		OnSuccess(func(response *client.Response) {
			a.Components().Set(component)
		})
}
