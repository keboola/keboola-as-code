package relationlink_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper/relationlink"
)

func TestRelationsMapper_Local_AfterOperation(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// Manifest side
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedManifestSideRelation{
				OtherSide: key2,
			},
		},
	}
	state.MustAdd(object1)
	state.NamingRegistry().MustAttach(key1, model.NewAbsPath("", "object1"))

	// API side
	object2 := &fixtures.MockedObject{
		MockedKey: key2,
		Relations: model.Relations{},
	}
	state.MustAdd(object2)
	state.NamingRegistry().MustAttach(key2, model.NewAbsPath("", "object2"))

	// No other side relation
	assert.Empty(t, object2.Relations)

	// Call AfterLocalOperation
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(object1)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Other side relation has been created
	assert.Equal(t, model.Relations{
		&fixtures.MockedApiSideRelation{
			OtherSide: key1,
		},
	}, object2.Relations)
}

func TestRelationsMapper_AfterOperation_Local_MissingOtherSide(t *testing.T) {
	t.Parallel()
	state, d := createLocalStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// Manifest side
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedManifestSideRelation{
				OtherSide: key2,
			},
		},
	}
	state.MustAdd(object1)
	state.NamingRegistry().MustAttach(key1, model.NewAbsPath("", "object1"))

	// Call AfterLocalOperation
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(object1)))

	// Warning is logged
	expected := `
WARN  Warning:
  - mocked key "456" not found
    - referenced from mocked key "123"
    - by relation "manifest_side_relation"
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func TestRelationsMapper_Remote_AfterOperation(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// API side
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedApiSideRelation{
				OtherSide: key2,
			},
		},
	}
	state.MustAdd(object1)

	// Manifest side
	object2 := &fixtures.MockedObject{
		MockedKey: key2,
		Relations: model.Relations{},
	}
	state.MustAdd(object2)

	// No other side relation
	assert.Empty(t, object2.Relations)

	// Call AfterLocalOperation
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(object1)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Other side relation has been created
	assert.Equal(t, model.Relations{
		&fixtures.MockedManifestSideRelation{
			OtherSide: key1,
		},
	}, object2.Relations)
}

func TestRelationsMapper_AfterOperation_Remote_MissingOtherSide(t *testing.T) {
	t.Parallel()
	state, d := createRemoteStateWithMapper(t)
	logger := d.DebugLogger()

	key1 := fixtures.MockedKey{Id: "123"}
	key2 := fixtures.MockedKey{Id: "456"}

	// API side
	object1 := &fixtures.MockedObject{
		MockedKey: key1,
		Relations: model.Relations{
			&fixtures.MockedApiSideRelation{
				OtherSide: key2,
			},
		},
	}
	state.MustAdd(object1)

	// Call AfterLocalOperation
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(object1)))

	// Warning is logged
	expected := `
WARN  Warning:
  - mocked key "456" not found
    - referenced from mocked key "123"
    - by relation "api_side_relation"
`
	assert.Equal(t, strings.TrimLeft(expected, "\n"), logger.AllMessages())
}

func createLocalStateWithMapper(t *testing.T) (*local.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyLocalState()
	mockedState.Mapper().AddMapper(relationlink.NewMapper(mockedState, d))
	return mockedState, d
}

func createRemoteStateWithMapper(t *testing.T) (*remote.State, *dependencies.TestContainer) {
	t.Helper()
	d := dependencies.NewTestContainer()
	mockedState := d.EmptyRemoteState()
	mockedState.Mapper().AddMapper(relationlink.NewMapper(mockedState, d))
	return mockedState, d
}
