package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func (a *StorageApi) ListComponents(branchId int) (*[]*model.Component, error) {
	response := a.ListComponentsRequest(branchId).Send().Response()
	if response.HasResult() {
		return response.Result().(*[]*model.Component), nil
	}
	return nil, response.Error()
}

func (a *StorageApi) ListComponentsRequest(branchId int) *client.Request {
	components := make([]*model.Component, 0)
	return a.
		NewRequest(resty.MethodGet, fmt.Sprintf("branch/%d/components", branchId)).
		SetQueryParam("include", "configuration,rows").
		SetResult(&components).
		OnSuccess(func(response *client.Response) *client.Response {
			if response.Result() != nil {
				// Add missing values
				for _, component := range components {
					// Set component.BranchId
					component.BranchId = branchId

					// Set config IDs
					for _, config := range component.Configs {
						config.BranchId = branchId
						config.ComponentId = component.Id

						// Set rows IDs
						for _, row := range config.Rows {
							row.BranchId = branchId
							row.ComponentId = component.Id
							row.ConfigId = config.Id
						}
					}
				}
			}
			return response
		})
}
