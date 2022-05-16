package service

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestCustomRepositoryPath(t *testing.T) {
	t.Parallel()

	logger := log.New(os.Stderr, "", 0)
	envs, err := env.FromOs()
	assert.NoError(t, err)

	d := dependencies.NewContainer(context.Background(), "file:///var/templates", false, logger, envs)
	res, err := repositories(d)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, model.RepositoryTypeDir, string(res[0].Type))
	assert.Equal(t, "/var/templates", res[0].Url)

	d = dependencies.NewContainer(context.Background(), "https://github.com/keboola/keboola-as-code-templates:main", false, logger, envs)
	res, err = repositories(d)
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, model.RepositoryTypeGit, string(res[0].Type))
	assert.Equal(t, "https://github.com/keboola/keboola-as-code-templates", res[0].Url)
	assert.Equal(t, "main", res[0].Ref)
}
