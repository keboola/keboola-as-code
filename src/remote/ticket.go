package remote

import (
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func (a *StorageApi) GenerateNewId() (*model.Ticket, error) {
	response := a.GenerateNewIdRequest().Send().Response
	if response.HasResult() {
		return response.Result().(*model.Ticket), nil
	}
	return nil, response.Err()
}

// GenerateNewIdRequest https://keboola.docs.apiary.io/#reference/tickets/generate-unique-id/generate-new-id
func (a *StorageApi) GenerateNewIdRequest() *client.Request {
	ticket := &model.Ticket{}
	return a.
		NewRequest(resty.MethodPost, "tickets").
		SetResult(ticket)
}
