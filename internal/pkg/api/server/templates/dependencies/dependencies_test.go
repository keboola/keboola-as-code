package dependencies

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

func TestRepositories(t *testing.T) {
	t.Parallel()

	testProject := testproject.GetTestProject(t, env.Empty())

	repositories := []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: repository.DefaultTemplateRepositoryName,
			Url:  repository.DefaultTemplateRepositoryUrl,
			Ref:  repository.DefaultTemplateRepositoryRefMain,
		},
	}
	envs := env.Empty()
	envs.Set("KBC_STORAGE_API_HOST", testProject.StorageAPIHost())

	// Init container
	ctx := context.Background()
	logger := log.New(os.Stderr, "[tests]", 0)
	c, err := NewContainer(ctx, repositories, false, false, logger, envs)
	assert.NoError(t, err)

	// Get API
	storageApiClient, err := c.StorageApiClient()
	assert.NoError(t, err)

	// Verify token
	token, err := storageapi.VerifyTokenRequest(testProject.StorageAPIToken().Token).Send(ctx, storageApiClient)
	assert.NoError(t, err)

	// Modify dependencies
	c, err = c.WithStorageApiClient(storageapi.ClientWithToken(storageApiClient.(client.Client), token.Token), token)
	assert.NoError(t, err)

	// Check that default repository is returned
	res, err := c.Repositories()
	assert.NoError(t, err)
	assert.Equal(t, []model.TemplateRepository{
		{
			Type: model.RepositoryTypeGit,
			Name: repository.DefaultTemplateRepositoryName,
			Url:  repository.DefaultTemplateRepositoryUrl,
			Ref:  repository.DefaultTemplateRepositoryRefMain,
		},
	}, res)
}
