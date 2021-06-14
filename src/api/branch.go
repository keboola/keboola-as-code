package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) ListBranches() ([]*remote.Branch, error) {
	response := a.ListBranchesReq().Send().Response()
	if response.HasResult() {
		return response.Result().([]*remote.Branch), nil
	}
	return nil, response.Error()
}

func (a *StorageApi) ListBranchesReq() *client.Request {
	return a.
		Request(resty.MethodGet, "dev-branches").
		SetResult([]*remote.Branch{}).
		OnSuccess(func(response *client.Response) *client.Response {
			if response.Result() != nil {
				// Map pointer to slice
				response.SetResult(*response.Result().(*[]*remote.Branch))
			}
			return response
		})

}

func (a *StorageApi) DeleteBranch(branchId int) *client.Response {
	return a.DeleteBranchReq(branchId).Send().Response()
}

func (a *StorageApi) DeleteBranchReq(branchId int) *client.Request {
	return a.Request(resty.MethodDelete, fmt.Sprintf("dev-branches/%d", branchId))
}
