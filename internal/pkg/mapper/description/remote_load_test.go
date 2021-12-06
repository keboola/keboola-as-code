package description

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDescriptionMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()

	apiObject := &model.Config{Description: "foo\nbar\n\r\t ", Content: orderedmap.New()}
	internalObject := apiObject.Clone().(*model.Config)
	recipe := &model.RemoteLoadRecipe{ApiObject: apiObject, InternalObject: internalObject}

	assert.NoError(t, NewMapper().MapAfterRemoteLoad(recipe))
	assert.Equal(t, "foo\nbar\n\r\t ", apiObject.Description)
	assert.Equal(t, "foo\nbar", internalObject.Description)
}
