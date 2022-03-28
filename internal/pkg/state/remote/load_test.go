package remote_test

import (
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestUnitOfWork_LoadAll(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, httpTransport, state := newTestUow(t, testMapperInst)

	// Mocked response: branches
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, []interface{}{
			map[string]interface{}{
				"id":   123,
				"name": "My branch",
			},
		}).Once(),
	)

	// Mocked response: config metadata
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, storageapi.ConfigMetadataResponse{}).Once(),
	)

	// Mocked response: components + configs
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []interface{}{
			map[string]interface{}{
				"id":   "foo.bar",
				"name": "Foo Bar",
				"configurations": []map[string]interface{}{
					{
						"id": "456",
						"configuration": map[string]interface{}{
							"key": "api value",
						},
					},
				},
			},
		}).Once(),
	)

	// Load all
	uow.LoadAll()
	assert.NoError(t, uow.Invoke())

	// Config has been loaded
	assert.Len(t, state.Configs(), 1)
	configRaw, found := state.Get(model.ConfigKey{
		BranchId:    123,
		ComponentId: `foo.bar`,
		Id:          `456`,
	})
	assert.True(t, found)
	config := configRaw.(*model.Config)

	// API response has been mapped
	assert.Equal(t, `internal name`, config.Name)
	assert.Equal(t, `{"key":"internal value","new":"value"}`, json.MustEncodeString(config.Content, false))

	// AfterRemoteOperation event has been called
	assert.Equal(t, []string{
		`loaded branch "123"`,
		`loaded config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestUnitOfWork_LoadAll_ConfigMetadata(t *testing.T) {
	t.Parallel()
	uow, httpTransport, state := newTestUow(t)

	// Mocked response: branches
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, []interface{}{
			map[string]interface{}{
				"id":   123,
				"name": "My branch",
			},
		}).Once(),
	)

	// Mocked response: config metadata
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, storageapi.ConfigMetadataResponse{
			storageapi.ConfigMetadataResponseItem{
				ComponentId: "foo.bar",
				ConfigId:    "456",
				Metadata: []storageapi.ConfigMetadata{
					{
						Id:        "1",
						Key:       "KBC.KaC.Meta",
						Value:     "value1",
						Timestamp: "xxx",
					},
					{
						Id:        "2",
						Key:       "KBC.KaC.Meta2",
						Value:     "value2",
						Timestamp: "xxx",
					},
				},
			},
		}),
	)

	// Mocked response: components + configs
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []interface{}{
			map[string]interface{}{
				"id":   "foo.bar",
				"name": "Foo Bar",
				"configurations": []map[string]interface{}{
					{
						"id":   "456",
						"name": "Config With Metadata",
						"configuration": map[string]interface{}{
							"key": "value",
						},
					},
					{
						"id":   "789",
						"name": "Config Without Metadata",
						"configuration": map[string]interface{}{
							"key": "value",
						},
					},
				},
			},
		}).Once(),
	)

	// Load all
	uow.LoadAll()
	assert.NoError(t, uow.Invoke())

	// Check
	config1Key := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo.bar",
		Id:          "456",
	}
	config2Key := model.ConfigKey{
		BranchId:    123,
		ComponentId: "foo.bar",
		Id:          "789",
	}
	assert.Equal(t, map[string]string{
		"KBC.KaC.Meta":  "value1",
		"KBC.KaC.Meta2": "value2",
	}, state.MustGet(config1Key).(*model.Config).Metadata)
	assert.Equal(t, map[string]string{}, state.MustGet(config2Key).(*model.Config).Metadata)
}
