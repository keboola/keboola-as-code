package codes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestSharedCodeLegacyRemoteCodeContent(t *testing.T) {
	t.Parallel()
	context, rowState := createTestFixtures(t, `keboola.snowflake-transformation`)
	rowState.Remote.Content.Set(model.SharedCodeContentKey, "SELECT 1; \n  SELECT 2; \n ")

	event := model.OnObjectsLoadEvent{
		StateType:  model.StateTypeRemote,
		NewObjects: []model.Object{rowState.Remote},
		AllObjects: context.State.RemoteObjects(),
	}
	assert.NoError(t, NewMapper(context).OnObjectsLoad(event))

	v, found := rowState.Remote.Content.Get(model.SharedCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{"SELECT 1;", "SELECT 2;"}, v)
}
