package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a StorageApi) WithToken(token *remote.Token) *StorageApi {
	a.token = token
	a.client.SetHeader("X-StorageApi-Token", token.Token)
	return &a
}

func (a *StorageApi) Token() *remote.Token {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}

	return a.token
}

func (a *StorageApi) ProjectId() int {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}
	return a.token.ProjectId()
}

func (a *StorageApi) ProjectName() string {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}
	return a.token.ProjectName()
}

func (a *StorageApi) GetToken(token string) (*remote.Token, error) {
	response := a.GetTokenRequest(token).Send().Response()
	if response.HasResult() {
		return response.Result().(*remote.Token), nil
	}
	return nil, response.Error()
}

func (a *StorageApi) GetTokenRequest(token string) *client.Request {
	return a.
		Request(resty.MethodGet, "tokens/verify").
		SetHeader("X-StorageApi-Token", token).
		SetResult(&remote.Token{})
}
