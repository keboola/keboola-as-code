package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) ListBranches() (*[]*remote.Branch, error) {
	response := a.ListBranchesRequest().Send().Response()
	if response.HasResult() {
		return response.Result().(*[]*remote.Branch), nil
	}
	return nil, response.Error()
}

// ListBranchesRequest https://keboola.docs.apiary.io/#reference/development-branches/branches/list-branches
func (a *StorageApi) ListBranchesRequest() *client.Request {
	branches := make([]*remote.Branch, 0)
	return a.
		Request(resty.MethodGet, "dev-branches").
		SetResult(&branches)

}

func (a *StorageApi) DeleteBranch(branchId int) *client.Response {
	return a.DeleteBranchRequest(branchId).Send().Response()
}

// DeleteBranchRequest https://keboola.docs.apiary.io/#reference/development-branches/branch-manipulation/delete-branch
func (a *StorageApi) DeleteBranchRequest(branchId int) *client.Request {
	return a.Request(resty.MethodDelete, fmt.Sprintf("dev-branches/%d", branchId))
}
