package links_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestRemoteLoadTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIDContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIDContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeId(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Remote.Content.Set(model.SharedCodeIDContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	expectedLogs := `
WARN  Warning:
- Missing shared code config "branch:123/component:keboola.shared-code/config:missing":
  - Referenced from config "branch:123/component:keboola.python-transformation-v2/config:001".
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())

	// Link to shared code is not set
	assert.Nil(t, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIDContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIDContentKey)
	assert.False(t, found)
}

func TestRemoteLoadTranWithSharedCode_InvalidSharedCodeRowId(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	// Shared code config with rows
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)

	// Create transformation with shared code
	transformation := createRemoteTranWithSharedCode(t, sharedCodeKey, sharedCodeRowsKeys, state)
	transformation.Remote.Content.Set(model.SharedCodeRowsIDContentKey, []any{`missing`}) // <<<<<<<<<<<

	// Invoke
	changes := model.NewRemoteChanges()
	changes.AddLoaded(transformation)
	require.NoError(t, state.Mapper().AfterRemoteOperation(context.Background(), changes))
	expectedLogs := `
WARN  Warning:
- Missing shared code config row "branch:123/component:keboola.shared-code/config:456/row:missing":
  - Referenced from config "branch:123/component:keboola.python-transformation-v2/config:001".
`
	assert.Equal(t, strings.TrimLeft(expectedLogs, "\n"), logger.AllMessagesTxt())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey}, transformation.Remote.Transformation.LinkToSharedCode)

	// Keys from Content are deleted
	_, found := transformation.Remote.Content.Get(model.SharedCodeIDContentKey)
	assert.False(t, found)
	_, found = transformation.Remote.Content.Get(model.SharedCodeRowsIDContentKey)
	assert.False(t, found)
}
