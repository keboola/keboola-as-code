package description_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestDescriptionMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, _ := createStateWithMapper(t)

	object := &model.Config{Description: "foo\nbar\n\r\t ", Content: orderedmap.New()}
	recipe := model.NewRemoteLoadRecipe(&model.ConfigManifest{}, object)

	require.NoError(t, state.Mapper().MapAfterRemoteLoad(context.Background(), recipe))
	assert.Equal(t, "foo\nbar", object.Description)
}
