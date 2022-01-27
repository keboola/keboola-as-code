package storageapi

import (
	"github.com/go-resty/resty/v2"

	client2 "github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (a *Api) GetComponent(componentId model.ComponentId) (*model.Component, error) {
	response := a.GetComponentRequest(componentId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Component), nil
	}
	return nil, response.Err()
}

// GetComponentRequest https://keboola.docs.apiary.io/#reference/components-and-configurations/get-component/get-component
func (a *Api) GetComponentRequest(componentId model.ComponentId) *client2.Request {
	component := &model.Component{}
	component.Id = componentId
	return a.
		NewRequest(resty.MethodGet, "components/{componentId}").
		SetPathParam("componentId", componentId.String()).
		SetResult(component).
		OnSuccess(func(response *client2.Response) {
			a.Components().Set(component)
		})
}
