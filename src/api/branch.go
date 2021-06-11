package api

import (
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) ListBranches() ([]*remote.Branch, error) {
	if res, err := a.ListBranchesReq().Send(); err != nil {
		return nil, err
	} else {
		return *res.Result().(*[]*remote.Branch), nil
	}
}

func (a *StorageApi) ListBranchesReq() *resty.Request {
	return a.
		Req(resty.MethodGet, "/dev-branches/").
		SetResult([]*remote.Branch{})
}
