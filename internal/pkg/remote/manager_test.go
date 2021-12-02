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
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/local"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

type testMapper struct {
	remoteChanges []string
}

func (*testMapper) MapBeforeRemoteSave(recipe *model.RemoteSaveRecipe) error {
	if config, ok := recipe.ApiObject.(*model.Config); ok {
		config.Name = "modified name"
		config.Content.Set(`key`, `api value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (*testMapper) MapAfterRemoteLoad(recipe *model.RemoteLoadRecipe) error {
	if config, ok := recipe.InternalObject.(*model.Config); ok {
		config.Name = "internal name"
		config.Content.Set(`key`, `internal value`)
		config.Content.Set(`new`, `value`)
	}
	return nil
}

func (t *testMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	for _, objectState := range changes.Created() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`created %s`, objectState.Desc()))
	}
	for _, objectState := range changes.Loaded() {
		t.remoteChanges = append(t.remoteChanges, fmt.Sprintf(`loaded %s`, objectState.Desc()))
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
	testMapperInst, uow, httpTransport, _ := newTestRemoteUOW(t)

	// Test object
	configKey := model.ConfigKey{BranchId: 123, ComponentId: `foo.bar`, Id: `456`}
	configState := &model.ConfigState{
		ConfigManifest: &model.ConfigManifest{
			ConfigKey: configKey,
		},
		Local: &model.Config{
			ConfigKey: configKey,
			Name:      "internal name",
			Content: utils.PairsToOrderedMap([]utils.Pair{
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
	assert.NoError(t, uow.Invoke())

	// Modified version was sent to the API
	reqBodyRaw, err := io.ReadAll(httpRequest.Body)
	assert.NoError(t, err)
	reqBody, err := url.QueryUnescape(string(reqBodyRaw))
	assert.NoError(t, err)
	assert.Contains(t, reqBody, `configuration={"key":"api value","new":"value"}`)
	assert.Contains(t, reqBody, `name=modified name`)

	// But the internal state is unchanged
	assert.Equal(t, `internal name`, configState.Local.Name)
	assert.Equal(t, `{"key":"internal value"}`, json.MustEncodeString(configState.Local.Content, false))

	// OnRemoteChange event has been called
	assert.Equal(t, []string{
		`created config "branch:123/component:foo.bar/config:456"`,
		`saved config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func TestRemoteLoadMapper(t *testing.T) {
	t.Parallel()
	testMapperInst, uow, httpTransport, state := newTestRemoteUOW(t)

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
	config := configRaw.(*model.ConfigState).Remote

	// API response has been mapped
	assert.Equal(t, `internal name`, config.Name)
	assert.Equal(t, `{"key":"internal value","new":"value"}`, json.MustEncodeString(config.Content, false))

	// OnRemoteChange event has been called
	assert.Equal(t, []string{
		`loaded branch "123"`,
		`loaded config "branch:123/component:foo.bar/config:456"`,
	}, testMapperInst.remoteChanges)
}

func newTestRemoteUOW(t *testing.T) (*testMapper, *remote.UnitOfWork, *httpmock.MockTransport, *model.State) {
	t.Helper()
	testMapperInst := &testMapper{}
	mappers := []interface{}{testMapperInst}
	storageApi, httpTransport, _ := testapi.NewMockedStorageApi()
	localManager, state := newTestLocalManager(t, mappers)
	mapperContext := model.MapperContext{
		Logger: zap.NewNop().Sugar(),
		Fs:     localManager.Fs(),
		Naming: localManager.Naming(),
		State:  state,
	}
	mapperInst := mapper.New(mapperContext).AddMapper(mappers...)

	remoteManager := remote.NewManager(localManager, storageApi, state, mapperInst)
	return testMapperInst, remoteManager.NewUnitOfWork(context.Background(), `change desc`), httpTransport, state
}

func newTestLocalManager(t *testing.T, mappers []interface{}) (*local.Manager, *model.State) {
	t.Helper()

	logger, _ := utils.NewDebugLogger()
	fs, err := aferofs.NewMemoryFs(logger, "")
	assert.NoError(t, err)

	m, err := manifest.NewManifest(1, "foo.bar", fs)
	assert.NoError(t, err)

	components := model.NewComponentsMap(testapi.NewMockedComponentsProvider())
	state := model.NewState(zap.NewNop().Sugar(), fs, components, model.SortByPath)
	mapperContext := model.MapperContext{Logger: logger, Fs: fs, Naming: m.Naming, State: state}
	return local.NewManager(logger, fs, m, state, mapper.New(mapperContext).AddMapper(mappers...)), state
}
