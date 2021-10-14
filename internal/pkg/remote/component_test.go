package remote_test

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testproject"
)

func TestGetComponent(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
	project := testproject.GetTestProject(t, env.Empty())
	api := project.Api()

	component, err := api.GetComponent("foo-bar")
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "Component foo-bar not found")
	assert.Nil(t, component)
}

func TestComponentIsDeprecated(t *testing.T) {
	t.Parallel()
	api, httpTransport, _ := testapi.TestMockedStorageApi()

	responder, err := httpmock.NewJsonResponder(200, map[string]interface{}{
		"id":   "wr-dropbox",
		"type": "writer",
		"name": "DropBox",
		"flags": []interface{}{
			model.DeprecatedFlag,
		},
	})
	assert.NoError(t, err)
	httpTransport.RegisterResponder("GET", `=~/storage/components/wr-dropbox`, responder)

	component, err := api.GetComponent("wr-dropbox")
	assert.NoError(t, err)
	assert.NotNil(t, component)
	assert.True(t, component.IsDeprecated())
}
