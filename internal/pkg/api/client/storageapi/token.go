package storageapi

import (
	"fmt"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (a Api) WithToken(token model.Token) *Api {
	a.token = &token
	a.client.SetHeader("X-StorageApi-Token", token.Token)
	return &a
}

func (a *Api) Token() model.Token {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}

	return *a.token
}

func (a *Api) ProjectId() int {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}
	return a.token.ProjectId()
}

func (a *Api) ProjectName() string {
	if a.token == nil {
		panic(fmt.Errorf("token is not set"))
	}
	return a.token.ProjectName()
}

func (a *Api) GetToken(token string) (model.Token, error) {
	response := a.GetTokenRequest(token).Send().Response
	if response.HasResult() {
		return *response.Result().(*model.Token), nil
	}
	return model.Token{}, response.Err()
}

func (a *Api) GetTokenRequest(token string) *client.Request {
	return a.
		NewRequest(resty.MethodGet, "tokens/verify").
		SetHeader("X-StorageApi-Token", token).
		SetResult(&model.Token{})
}
