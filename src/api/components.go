package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) ListComponents(branchId int) ([]*remote.Component, error) {
	response := a.ListComponentsReq(branchId).Send().Response()
	if response.HasResult() {
		return response.Result().([]*remote.Component), nil
	}
	return nil, response.Error()
}

func (a *StorageApi) ListComponentsReq(branchId int) *client.Request {
	return a.
		Request(resty.MethodGet, fmt.Sprintf("branch/%d/components", branchId)).
		SetQueryParam("include", "configuration,rows").
		SetResult([]*remote.Component{}).
		SetDecorator(func(response *resty.Response, err error) (*resty.Response, error) {
			if err == nil && response != nil && response.Result() != nil {
				// Map pointer to slice
				components := *response.Result().(*[]*remote.Component)

				// Add missing values
				for _, component := range components {
					// Set component.BranchId
					component.BranchId = branchId

					// Set configuration.BranchId and ComponentId
					for _, configuration := range component.Configurations {
						configuration.BranchId = branchId
						configuration.ComponentId = component.Id
					}
				}

				response.Request.Result = components
			}
			return response, err
		})
}
