package remote_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	validatorPkg "github.com/keboola/keboola-as-code/internal/pkg/validator"
)

type testMapper struct {
	remoteChanges []string
}

func (*testMapper) MapBeforeRemoteSave(ctx context.Context, recipe *model.RemoteSaveRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "modified name"
		config.Content.Set(`key`, `api value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (*testMapper) MapAfterRemoteLoad(ctx context.Context, recipe *model.RemoteLoadRecipe) error {
	if config, ok := recipe.Object.(*model.Config); ok {
		config.Name = "internal name"
		config.Content.Set(`key`, `internal value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (t *testMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	for _, objectState := range changes.Loaded() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`loaded %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Created() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`created %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Updated() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`updated %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Saved() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`saved %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Deleted() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`deleted %s`, objectState.Desc()))
	}
	return nil
}

func TestRemoteSaveMapper(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, httpTransport, _ := newTestRemoteUOW(t, testMapperInst)

	// Test object
	configKey := model.ConfigKey{BranchID: 123, ComponentID: `foo.bar`, ID: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "internal name",
			Content: orderedmap.FromPairs([]orderedmap.Pair{
				{Key: "key", Value: "internal value"},
			}),
		},
	}

	// Mocked response: create config
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			return httpmock.NewStringResponse(201, `{"id": "456"}`), nil
		},
	)

	// Save object
	uow.SaveObject(configState, configState.Local, model.ChangedFields{})
	require.NoError(t, uow.Invoke())

	// Modified version was sent to the API
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	require.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	require.NoError(t, err)
	assert.Contains(t, reqBody, "\"configuration\":{\"key\"\n:\"api value\"\n,\"new\"\n:\"value\"\n}")
	assert.Contains(t, reqBody, `"name":"modified name"`)

	// But the internal state is unchanged
	assert.Equal(t, `internal name`, configState.Local.Name)
	assert.Equal(t, `{"key":"internal value"}`, json.MustEncodeString(configState.Local.Content, false))

	// AfterRemoteOperation event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestRemoteLoadMapper(t *testing.T) {
	t.Parallel()
	testMapperInst := &testMapper{}
	uow, httpTransport, projectState := newTestRemoteUOW(t, testMapperInst)

	// Mocked response: branches
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, []any{
			map[string]any{
				"id":   123,
				"name": "My branch",
			},
		}).Once(),
	)

	// Mocked response: branch metadata
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/metadata`,
		httpmock.NewJsonResponderOrPanic(200, []keboola.MetadataDetail{
			{
				ID:        "1",
				Key:       "KBC.KaC.branch-meta",
				Value:     "val1",
				Timestamp: "xxx",
			},
		}).Once(),
	)

	// Mocked response: config metadata
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, []keboola.ConfigMetadataItem{}).Once(),
	)

	// Mocked response: components + configs
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/components`,
		httpmock.NewJsonResponderOrPanic(200, []any{
			map[string]any{
				"id":   "foo.bar",
				"name": "Foo Bar",
				"configurations": []map[string]any{
					{
						"id": "456",
						"configuration": map[string]any{
							"key": "api value",
						},
					},
				},
			},
		}).Once(),
	)

	// Load all
	uow.LoadAll(model.NoFilter())
	require.NoError(t, uow.Invoke())

	// Config has been loaded
	assert.Len(t, projectState.Configs(), 1)
	configRaw, found := projectState.Get(model.ConfigKey{
		BranchID:    123,
		ComponentID: `foo.bar`,
		ID:          `456`,
	})
	assert.True(t, found)
	config := configRaw.(*model.ConfigState).Remote

	// API response has been mapped
	assert.Equal(t, `internal name`, config.Name)
	assert.Equal(t, `{"key":"internal value","new":"value"}`, json.MustEncodeString(config.Content, false))

	// AfterRemoteOperation event has been called
	assert.Equal(t, []string{
		`loaded branch "123"`,
		`loaded config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestLoadConfigMetadata(t *testing.T) {
	t.Parallel()
	uow, httpTransport, projectState := newTestRemoteUOW(t)

	// Mocked response: branches
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/dev-branches`,
		httpmock.NewJsonResponderOrPanic(200, []any{
			map[string]any{
				"id":   123,
				"name": "My branch",
			},
		}).Once(),
	)

	// Mocked response: branch metadata
	httpTransport.RegisterResponder(
		resty.MethodGet,
		`=~storage/branch/123/metadata`,
		httpmock.NewJsonResponderOrPanic(200, keboola.MetadataDetails{
			{
				ID:        "1",
				Key:       "KBC.KaC.branch-meta",
				Value:     "val1",
				Timestamp: "xxx",
			},
		}).Once(),
	)

	// Mocked response: config metadata
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, []keboola.ConfigMetadataItem{
			{
				ComponentID: "foo.bar",
				ConfigID:    "456",
				Metadata: keboola.MetadataDetails{
					{
						ID:        "1",
						Key:       "KBC.KaC.Meta",
						Value:     "value1",
						Timestamp: "xxx",
					},
					{
						ID:        "2",
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
		httpmock.NewJsonResponderOrPanic(200, []any{
			map[string]any{
				"id":   "foo.bar",
				"name": "Foo Bar",
				"configurations": []map[string]any{
					{
						"id":   "456",
						"name": "Config With Metadata",
						"configuration": map[string]any{
							"key": "value",
						},
					},
					{
						"id":   "789",
						"name": "Config Without Metadata",
						"configuration": map[string]any{
							"key": "value",
						},
					},
				},
			},
		}).Once(),
	)

	// Load all
	uow.LoadAll(model.NoFilter())
	require.NoError(t, uow.Invoke())

	// Check
	config1Key := model.ConfigKey{
		BranchID:    123,
		ComponentID: "foo.bar",
		ID:          "456",
	}
	config2Key := model.ConfigKey{
		BranchID:    123,
		ComponentID: "foo.bar",
		ID:          "789",
	}
	assert.Equal(t, model.ConfigMetadata{
		"KBC.KaC.Meta":  "value1",
		"KBC.KaC.Meta2": "value2",
	}, projectState.MustGet(config1Key).(*model.ConfigState).Remote.Metadata)
	assert.Equal(t, model.ConfigMetadata{}, projectState.MustGet(config2Key).(*model.ConfigState).Remote.Metadata)
	branchKey := model.BranchKey{ID: 123}
	assert.Equal(t, model.BranchMetadata{
		"KBC.KaC.branch-meta": "val1",
	}, projectState.MustGet(branchKey).(*model.BranchState).Remote.Metadata)
}

func TestSaveConfigMetadata_Create(t *testing.T) {
	t.Parallel()
	uow, httpTransport, _ := newTestRemoteUOW(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs$`,
		httpmock.NewStringResponder(201, `{"id": "456"}`),
	)

	// Mocked response: append metadata
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~/storage/branch/123/components/foo.bar/configs/456/metadata$`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			response := keboola.MetadataDetails{
				{ID: "1", Key: "KBC-KaC-meta1", Value: "val1", Timestamp: "xxx"},
			}
			return httpmock.NewStringResponse(200, json.MustEncodeString(response, true)), nil
		},
	)

	// Fixtures
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
	config := &model.Config{
		ConfigKey: configKey,
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}
	objectState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          config,
	}

	// Save
	uow.SaveObject(objectState, objectState.Local, model.NewChangedFields())
	require.NoError(t, uow.Invoke())

	// Check
	assert.NotNil(t, httpRequest)
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	require.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	require.NoError(t, err)
	assert.Equal(t, `{"metadata":[{"key":"KBC-KaC-meta1","value":"val1"}]}`, reqBody)
}

func TestSaveConfigMetadata_Create_Empty(t *testing.T) {
	t.Parallel()
	uow, httpTransport, _ := newTestRemoteUOW(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPost, `=~storage/branch/123/components/foo.bar/configs$`,
		httpmock.NewStringResponder(201, `{"id": "456"}`),
	)

	// Fixtures
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
	config := &model.Config{
		ConfigKey: configKey,
		Metadata:  map[string]string{},
	}
	objectState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          config,
	}

	// Save
	uow.SaveObject(objectState, objectState.Local, model.NewChangedFields())
	require.NoError(t, uow.Invoke())
}

func TestSaveConfigMetadata_Update(t *testing.T) {
	t.Parallel()
	uow, httpTransport, _ := newTestRemoteUOW(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPut, `=~storage/branch/123/components/foo.bar/configs/456$`,
		httpmock.NewStringResponder(200, `{"id": "456"}`),
	)

	// Mocked response: append metadata
	var httpRequest *http.Request
	httpTransport.RegisterResponder(resty.MethodPost, `=~/storage/branch/123/components/foo.bar/configs/456/metadata$`,
		func(req *http.Request) (*http.Response, error) {
			httpRequest = req
			response := keboola.MetadataDetails{
				{ID: "1", Key: "KBC-KaC-meta1", Value: "val1", Timestamp: "xxx"},
			}
			return httpmock.NewStringResponse(200, json.MustEncodeString(response, true)), nil
		},
	)

	// Fixtures
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
	config := &model.Config{
		ConfigKey: configKey,
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}
	objectState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          config,
		Remote:         config,
	}

	// Save
	uow.SaveObject(objectState, objectState.Local, model.NewChangedFields("metadata"))
	require.NoError(t, uow.Invoke())

	// Check
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	require.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	require.NoError(t, err)
	assert.Equal(t, `{"metadata":[{"key":"KBC-KaC-meta1","value":"val1"}]}`, reqBody)
}

func TestSaveConfigMetadata_Update_NoChange(t *testing.T) {
	t.Parallel()
	uow, httpTransport, _ := newTestRemoteUOW(t)

	// Mocked response: create config
	httpTransport.RegisterResponder(resty.MethodPut, `=~storage/branch/123/components/foo.bar/configs/456$`,
		httpmock.NewStringResponder(200, `{"id": "456"}`),
	)

	// Fixtures
	configKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "456"}
	config := &model.Config{
		ConfigKey: configKey,
		Metadata: map[string]string{
			"KBC-KaC-meta1": "val1",
		},
	}
	objectState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{ConfigKey: configKey},
		Local:          config,
		Remote:         config,
	}

	// Save, metadata field is not changed
	uow.SaveObject(objectState, objectState.Local, model.NewChangedFields())
	require.NoError(t, uow.Invoke())
}

func newTestRemoteUOW(t *testing.T, mappers ...any) (*remote.UnitOfWork, *httpmock.MockTransport, *state.Registry) {
	t.Helper()
	c, httpTransport := client.NewMockedClient()
	httpTransport.RegisterResponder(resty.MethodGet, `/v2/storage/?exclude=components`,
		httpmock.NewStringResponder(200, `{
			"services": [],
			"features": []
		}`),
	)

	api, err := keboola.NewAuthorizedAPI(context.Background(), "https://connection.keboola.com", "my-token", keboola.WithClient(&c))
	require.NoError(t, err)
	localManager, projectState := newTestLocalManager(t, mappers)
	mapperInst := mapper.New().AddMapper(mappers...)

	remoteManager := remote.NewManager(localManager, api, projectState, mapperInst)
	return remoteManager.NewUnitOfWork(context.Background(), `change desc`), httpTransport, projectState
}

func newTestLocalManager(t *testing.T, mappers []any) (*local.Manager, *state.Registry) {
	t.Helper()

	logger := log.NewDebugLogger()
	validator := validatorPkg.New()
	fs := aferofs.NewMemoryFs(filesystem.WithLogger(logger))

	m := manifest.New(1, "foo.bar")
	projectState := state.NewRegistry(knownpaths.NewNop(context.Background()), naming.NewRegistry(), testapi.MockedComponentsMap(), model.SortByPath)

	namingTemplate := naming.TemplateWithIds()
	namingRegistry := naming.NewRegistry()
	namingGenerator := naming.NewGenerator(namingTemplate, namingRegistry)
	return local.NewManager(logger, validator, fs, fs.FileLoader(), m, namingGenerator, projectState, mapper.New().AddMapper(mappers...)), projectState
}
