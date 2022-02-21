package configmetadata_test

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testdeps"
)

func InitState(t *testing.T) (*state.State, *httpmock.MockTransport) {
	t.Helper()

	d := testdeps.New()
	_, httpTransport := d.UseMockedStorageApi()
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, storageapi.ConfigMetadataResponse{
			storageapi.ConfigMetadataResponseItem{
				ComponentId: "keboola.ex-aws-s3",
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
	httpTransport.RegisterResponder(
		"POST", `=~/storage/branch/123/components/keboola.ex-aws-s3/configs/456/metadata`,
		httpmock.NewJsonResponderOrPanic(200, []storageapi.ConfigMetadata{
			{
				Id:        "1",
				Key:       "KBC-KaC-meta1",
				Value:     "val1",
				Timestamp: "xxx",
			},
		}),
	)
	mockedState := d.EmptyState()
	assert.NoError(t, mockedState.Set(&model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: model.BranchKey{Id: 123}},
		Remote:         &model.Branch{BranchKey: model.BranchKey{Id: 123}},
	}))
	mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))
	return mockedState, httpTransport
}

func TestConfigMetadataOnRemoteChangeSaved(t *testing.T) {
	t.Parallel()
	mockedState, httpTransport := InitState(t)

	content := orderedmap.New()
	json.MustDecodeString("{}", content)

	// Config with metadata
	configKey := model.ConfigKey{
		BranchId:    123,
		ComponentId: "keboola.ex-aws-s3",
		Id:          "456",
	}
	configManifest := &model.ConfigManifest{
		ConfigKey: configKey,
	}
	config := &model.Config{ConfigKey: configKey, Content: content, Metadata: map[string]string{"KBC.KaC.meta1": "val1"}}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, mockedState.Set(configState))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddCreated(configState)
	assert.NoError(t, mockedState.Mapper().AfterRemoteOperation(changes))
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["POST =~/storage/branch/123/components/keboola.ex-aws-s3/configs/456/metadata"])
}
