package remote_test

import (
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestUnitOfWork_SaveObject_Create(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, httpTransport, state := newTestUow(t, testMapperInst)

	// Test object
	config := &model.Config{
		ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, ConfigId: `456`},
		Name:      "internal name",
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "internal value"},
		}),
	}

	// Add parent branch
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})

	// Mocked response: create config
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(201, `{"id": "456"}`), nil
		},
	)

	// Save object
	uow.Save(config, model.NewChangedFields())
	assert.NoError(t, uow.Invoke())

	// Modified version was sent to the API
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	assert.NoError(t, err)
	assert.Contains(t, reqBody, `configuration={"key":"api value","new":"value"}`)
	assert.Contains(t, reqBody, `name=modified name`)

	// But the internal state is unchanged
	assert.Equal(t, `internal name`, config.Name)
	assert.Equal(t, `{"key":"internal value"}`, json.MustEncodeString(config.Content, false))

	// AfterRemoteOperation event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestUnitOfWork_SaveObject_Update(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, httpTransport, state := newTestUow(t, testMapperInst)

	// Test object
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, ConfigId: `456`}
	config := &model.Config{
		ConfigKey: configKey,
		Name:      "internal name",
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{Key: "key", Value: "internal value"},
		}),
	}

	// Config already exists
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})
	state.MustAdd(&model.Config{
		ConfigKey: configKey,
		Name:      "old name",
	})

	// Mocked response: create config
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPut, `=~storage/branch/123/components/foo.bar/configs`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(201, `{"id": "456"}`), nil
		},
	)

	// Save object
	uow.Save(config, model.NewChangedFields("name", "content"))
	assert.NoError(t, uow.Invoke())

	// Modified version was sent to the API
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	assert.NoError(t, err)
	assert.Contains(t, reqBody, `configuration={"key":"api value","new":"value"}`)
	assert.Contains(t, reqBody, `name=modified name`)

	// But the internal state is unchanged
	assert.Equal(t, `internal name`, config.Name)
	assert.Equal(t, `{"key":"internal value"}`, json.MustEncodeString(config.Content, false))

	// AfterRemoteOperation event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestUnitOfWork_SaveConfigMetadata_Create(t *testing.T) {
	t.Parallel()
	uow, httpTransport, state := newTestUow(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs$`,
		httpmock.NewStringResponder(201, `{"id": "456"}`),
	)

	// Mocked response: append metadata
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~/storage/branch/123/components/foo.bar/configs/456/metadata$`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			response := []storageapi.ConfigMetadata{
				{Id: "1", Key: "KBC-KaC-meta1", Value: "val1", Timestamp: "xxx"},
			}
			return httpmock.NewStringResponse(200, json.MustEncodeString(response, true)), nil
		},
	)

	// Fixtures
	config := &model.Config{
		ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "456"},
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}

	// Add parent branch
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})

	// Save
	uow.Save(config, model.NewChangedFields())
	assert.NoError(t, uow.Invoke())

	// Check
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	assert.NoError(t, err)
	assert.Equal(t, "metadata[0][key]=KBC-KaC-meta1&metadata[0][value]=val1", reqBody)
}

func TestUnitOfWork_SaveConfigMetadata_Create_Empty(t *testing.T) {
	t.Parallel()
	uow, httpTransport, state := newTestUow(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs$`,
		httpmock.NewStringResponder(201, `{"id": "456"}`),
	)

	// Fixtures
	config := &model.Config{
		ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "456"},
		Metadata:  map[string]string{},
	}
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})

	// Save, no metadata request
	uow.Save(config, model.NewChangedFields())
	assert.NoError(t, uow.Invoke())
}

func TestUnitOfWork_SaveConfigMetadata_Update(t *testing.T) {
	t.Parallel()
	uow, httpTransport, state := newTestUow(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPut, `=~storage/branch/123/components/foo.bar/configs/456$`,
		httpmock.NewStringResponder(200, `{"id": "456"}`),
	)

	// Mocked response: append metadata
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~/storage/branch/123/components/foo.bar/configs/456/metadata$`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			response := []storageapi.ConfigMetadata{
				{Id: "1", Key: "KBC-KaC-meta1", Value: "val1", Timestamp: "xxx"},
			}
			return httpmock.NewStringResponse(200, json.MustEncodeString(response, true)), nil
		},
	)

	// Fixtures
	config := &model.Config{
		ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "456"},
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})
	state.MustAdd(config)

	// Save
	uow.Save(config, model.NewChangedFields("metadata"))
	assert.NoError(t, uow.Invoke())

	// Check
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	assert.NoError(t, err)
	assert.Equal(t, "metadata[0][key]=KBC-KaC-meta1&metadata[0][value]=val1", reqBody)
}

func TestUnitOfWork_SaveConfigMetadata_Update_NoChange(t *testing.T) {
	t.Parallel()
	uow, httpTransport, state := newTestUow(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPut, `=~storage/branch/123/components/foo.bar/configs/456$`,
		httpmock.NewStringResponder(200, `{"id": "456"}`),
	)

	// Fixtures
	config := &model.Config{
		ConfigKey: model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", ConfigId: "456"},
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}
	state.MustAdd(&model.Branch{BranchKey: model.BranchKey{BranchId: config.BranchId}})
	state.MustAdd(config)

	// Save, metadata field is not changed
	uow.Save(config, model.NewChangedFields())
	assert.NoError(t, uow.Invoke())
}
