package remote_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestGetComponent(t *testing.T) {
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	component, err := api.GetComponent("ex-generic-v2")
	assert.False(t, component.IsDeprecated())
	assert.NoError(t, err)
	assert.NotNil(t, component)
	assert.Equal(t, "ex-generic-v2", component.Id)
	assert.Equal(t, "extractor", component.Type)
}

func TestGetComponentNotFound(t *testing.T) {
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	component, err := api.GetComponent("foo-bar")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Component foo-bar not found")
	assert.Nil(t, component)
}

func TestComponentIsDeprecated(t *testing.T) {
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	component, err := api.GetComponent("wr-dropbox")
	assert.NoError(t, err)
	assert.NotNil(t, component)
	assert.True(t, component.IsDeprecated())
}
