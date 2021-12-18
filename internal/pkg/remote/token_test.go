package remote_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestApiWithToken(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	token := model.Token{Id: "123", Token: "mytoken", Owner: model.TokenOwner{Id: 456, Name: "name"}}
	orgApi := NewStorageApi("foo.bar.com", context.Background(), logger, false)
	tokenApi := orgApi.WithToken(token)

	// Must be cloned, not modified
	assert.NotEqual(t, orgApi, tokenApi)
	assert.Equal(t, token, tokenApi.Token())
	assert.Equal(t, "mytoken", tokenApi.RestyClient().Header.Get("X-StorageApi-Token"))
}

func TestGetToken(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	logger := log.NewDebugLogger()
	api := NewStorageApi(project.StorageApiHost(), context.Background(), logger, false)

	tokenValue := project.Token()
	token, err := api.GetToken(tokenValue)
	assert.NoError(t, err)
	assert.Regexp(t, `DEBUG  HTTP      GET https://.*/v2/storage/tokens/verify | 200 | .*`, logger.String())
	assert.Equal(t, tokenValue, token.Token)
	assert.Equal(t, project.Id(), token.ProjectId())
	assert.NotEmpty(t, token.ProjectName())
}

func TestGetTokenEmpty(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	logger := log.NewDebugLogger()
	api := NewStorageApi(project.StorageApiHost(), context.Background(), logger, false)

	tokenValue := ""
	token, err := api.GetToken(tokenValue)
	assert.Error(t, err)
	apiErr := err.(*Error)
	assert.Equal(t, "Access token must be set", apiErr.Message)
	assert.Equal(t, "", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.HttpStatus())
	assert.Empty(t, token)
}

func TestGetTokenInvalid(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	logger := log.NewDebugLogger()
	api := NewStorageApi(project.StorageApiHost(), context.Background(), logger, false)

	tokenValue := "mytoken"
	token, err := api.GetToken(tokenValue)
	assert.Error(t, err)
	apiErr := err.(*Error)
	assert.Equal(t, "Invalid access token", apiErr.Message)
	assert.Equal(t, "storage.tokenInvalid", apiErr.ErrCode)
	assert.Equal(t, 401, apiErr.HttpStatus())
	assert.Empty(t, token)
}
