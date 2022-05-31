package storageapi

import (
	"github.com/keboola/keboola-as-code/internal/pkg/http"
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
func (a *Api) GetComponentRequest(componentId model.ComponentId) *http.Request {
	component := &model.Component{}
	component.Id = componentId
	return a.
		NewRequest(http.MethodGet, "components/{componentId}").
		SetPathParam("componentId", componentId.String()).
		SetResult(component).
		OnSuccess(func(response *http.Response) {
			a.Components().Set(component)
		})
}
