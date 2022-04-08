package links_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLocalLoadTranWithSharedCode(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)
	transformation := createLocalTransformationWithSharedCode(t, state)

	// Invoke
	assert.NoError(t, state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(transformation)))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Paths in transformation blocks are replaced by IDs
	blockKey := model.BlockKey{Parent: transformation.ConfigKey, Index: 0}
	assert.Equal(t, []*model.Block{
		{
			BlockKey: blockKey,
			Name:     `Block 1`,
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 0},
					Name:    `Code 1`,
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								ConfigRowId: `1234`,
							},
						},
					},
				},
				{
					CodeKey: model.CodeKey{Parent: blockKey, Index: 1},
					Name:    `Code 2`,
					Scripts: model.Scripts{
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								ConfigRowId: `5678`,
							},
						},
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								ConfigRowId: `1234`,
							},
						},
					},
				},
			},
		},
	}, transformation.Transformation.Blocks)

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodePath(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	fixtures.CreateSharedCode(t, state)
	transformation := createLocalTransformationWithSharedCode(t, state)
	transformation.Content.Set(model.SharedCodePathContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	expectedErr := `
missing shared code "branch/missing":
  - referenced from config "branch:123/component:keboola.python-transformation-v2/config:789"
`
	err := state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(transformation))
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Link to shared code is not set
	assert.Nil(t, transformation.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodeRowPath(t *testing.T) {
	t.Parallel()
	state, d := createStateWithLocalMapper(t)
	logger := d.DebugLogger()

	// Create transformation with shared code
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, state)
	transformation := createLocalTransformationWithSharedCode(t, state)
	transformation.Transformation.Blocks[0].Codes[1].Scripts[0] = model.StaticScript{Value: "# {{:codes/missing}}\n"} // <<<<<<<<<<<<

	// Invoke
	err := state.Mapper().AfterLocalOperation(model.NewChanges().AddLoaded(transformation))
	assert.Error(t, err)

	// Check error
	expectedErr := `
missing shared code "branch/_shared/keboola.python-transformation-v2/codes/missing":
  - referenced from "branch/transformation/blocks/block-1/code-2"
`
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: []model.ConfigRowKey{sharedCodeRowsKeys[0]}}, transformation.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}
