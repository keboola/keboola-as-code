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

	changes := model.NewRemoteChanges()
	changes.AddLoaded(rowState)
	assert.NoError(t, NewMapper(context).OnRemoteChange(changes))

	v, found := rowState.Remote.Content.Get(model.SharedCodeContentKey)
	assert.True(t, found)
	assert.Equal(t, []interface{}{"SELECT 1;", "SELECT 2;"}, v)
}
