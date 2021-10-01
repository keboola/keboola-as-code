package remote

import (
	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Index struct {
	Components []*model.Component
}

func (a *StorageApi) ListAllComponents() ([]*model.Component, error) {
	response := a.IndexRequest().Send().Response
	if response.HasResult() {
		return response.Result().(*Index).Components, nil
	}
	return nil, response.Err()
}

func (a *StorageApi) IndexRequest() *client.Request {
	index := &Index{}
	return a.
		NewRequest(resty.MethodGet, "").
		SetQueryParam(`exclude`, `componentDetails`).
		SetResult(index)
}
