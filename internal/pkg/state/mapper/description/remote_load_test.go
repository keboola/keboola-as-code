package description_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/description"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDescriptionMapAfterRemoteLoad(t *testing.T) {
	t.Parallel()
	state, _ := createStateWithMapper(t)

	object := &model.Config{Description: "foo\nbar\n\r\t ", Content: orderedmap.New()}

	_, err := state.Mapper().MapAfterRemoteLoad(object)
	assert.NoError(t, err)
	assert.Equal(t, "foo\nbar", object.Description)
}

func createStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(description.NewMapper())
	return mockedState, d
}
