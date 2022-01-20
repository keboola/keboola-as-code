package configmetadata_test

import (
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/testdeps"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestConfigMetadataMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	t.Helper()
	d := testdeps.New()
	_, httpTransport := d.UseMockedStorageApi()
	httpTransport.RegisterResponder(
		"GET", `=~/storage/branch/123/search/component-configurations`,
		httpmock.NewJsonResponderOrPanic(200, remote.ConfigMetadataResponse{
			remote.ConfigMetadataWrapper{
				ComponentId: "keboola.ex-aws-s3",
				ConfigId:    "456",
				Metadata: []remote.ConfigMetadata{
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
	mockedState := d.EmptyState()
	assert.NoError(t, mockedState.Set(&model.BranchState{
		BranchManifest: &model.BranchManifest{BranchKey: model.BranchKey{Id: 123}},
		Remote:         &model.Branch{BranchKey: model.BranchKey{Id: 123}},
	}))
	mockedState.Mapper().AddMapper(configmetadata.NewMapper(mockedState, d))

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
	config := &model.Config{ConfigKey: configKey, Content: content}
	configState := &model.ConfigState{
		ConfigManifest: configManifest,
		Remote:         config,
	}
	assert.NoError(t, mockedState.Set(configState))

	// Config without metadata
	configKey2 := model.ConfigKey{
		BranchId:    123,
		ComponentId: "keboola.ex-aws-s3",
		Id:          "789",
	}
	configManifest2 := &model.ConfigManifest{
		ConfigKey: configKey2,
	}
	config2 := &model.Config{ConfigKey: configKey2, Content: content}
	configState2 := &model.ConfigState{
		ConfigManifest: configManifest2,
		Remote:         config2,
	}
	assert.NoError(t, mockedState.Set(configState2))

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(configState)
	changes.AddLoaded(configState2)

	assert.NoError(t, mockedState.Mapper().OnRemoteChange(changes))
	assert.Equal(t, map[string]string{"KBC.KaC.Meta": "value1", "KBC.KaC.Meta2": "value2"}, config.Metadata)
	assert.Equal(t, map[string]string(nil), config2.Metadata)
}
