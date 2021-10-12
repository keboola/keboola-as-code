package remote_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestListAllComponents(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	components, err := api.ListAllComponents()
	assert.NoError(t, err)
	assert.Greater(t, len(components), 0)
}

func TestNewComponentList(t *testing.T) {
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	components, err := api.NewComponentList()
	assert.NoError(t, err)
	assert.Greater(t, len(components), 0)
	assert.True(t, strings.HasPrefix(components[0].Id, `keboola.`))
}
