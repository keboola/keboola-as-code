package service

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
)

func TestCustomRepositoryPath(t *testing.T) {
	t.Parallel()

	res, err := repositories("")
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, repository.DefaultTemplateRepositoryUrl, res[0].Url)

	res, err = repositories("file:///var/templates")
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, model.RepositoryTypeDir, string(res[0].Type))
	assert.Equal(t, "/var/templates", res[0].Path)

	res, err = repositories("https://github.com/keboola/keboola-as-code-templates:main")
	assert.NoError(t, err)
	assert.Len(t, res, 1)
	assert.Equal(t, model.RepositoryTypeGit, string(res[0].Type))
	assert.Equal(t, "https://github.com/keboola/keboola-as-code-templates", res[0].Url)
	assert.Equal(t, "main", res[0].Ref)
}
