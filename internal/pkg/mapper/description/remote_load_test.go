package description

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDescriptionMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()

	object := &model.Config{Description: "foo\nbar\n\r\t ", Content: orderedmap.New()}
	recipe := &model.RemoteLoadRecipe{Object: object}

	assert.NoError(t, NewMapper().MapAfterRemoteLoad(recipe))
	assert.Equal(t, "foo\nbar", object.Description)
}
