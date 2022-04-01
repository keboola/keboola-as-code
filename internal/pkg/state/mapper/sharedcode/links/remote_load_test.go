package links_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRemoteLoadTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(transformation)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeId(t *testing.T) {
	t.Parallel()
	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Content.Set(model.SharedCodeIdContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(transformation)))
	expectedLogs := `
WARN  Warning:
  - missing shared code config "branch:123/component:keboola.shared-code/config:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:001"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Link to shared code is not set
	assert.Nil(t, transformation.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeRowId(t *testing.T) {
	t.Parallel()
	state, d := createStateWithRemoteMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTransformationWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Content.Set(model.SharedCodeRowsIdContentKey, []interface{}{`missing`}) // <<<<<<<<<<<

	// Invoke
	assert.NoError(t, state.Mapper().AfterRemoteOperation(model.NewChanges().AddLoaded(transformation)))
	expectedLogs := `
WARN  Warning:
  - missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
    - referenced from config "branch:123/component:keboola.python-transformation-v2/config:001"
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessages())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey}, transformation.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Content.Get(model.SharedCodeIdContentKey)
	assert.False(t, found)
	_, found = transformation.Content.Get(model.SharedCodeRowsIdContentKey)
	assert.False(t, found)
}
