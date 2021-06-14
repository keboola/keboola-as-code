package api

import (
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) ListBranches() ([]*remote.Branch, error) {
	response, err := a.Send(a.ListBranchesReq())
	if err == nil {
		return response.Result().([]*remote.Branch), nil
	}
	return nil, err
}

func (a *StorageApi) ListBranchesReq() *client.Request {
	return client.NewRequestWithDecorator(
		a.Req(resty.MethodGet, "dev-branches").SetResult([]*remote.Branch{}),
		func(response *resty.Response, err error) (*resty.Response, error) {
			if err == nil && response != nil && response.Result() != nil {
				// Map pointer to slice
				response.Request.Result = *response.Result().(*[]*remote.Branch)
			}
			return response, err
		},
	)
}
