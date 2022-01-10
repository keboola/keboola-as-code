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
	mapperInst, context, logs := createMapper(t)
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createLocalTranWithSharedCode(t, context)

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	assert.NoError(t, mapperInst.OnLocalChange(changes))
	assert.Empty(t, logs.AllMessages())

	// Paths in transformation blocks are replaced by IDs
	assert.Equal(t, []*model.Block{
		{
			Name:          `Block 1`,
			PathInProject: model.NewPathInProject(`branch/transformation/blocks`, `block-1`),
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name:          `Code 1`,
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-1`),
					Scripts: model.Scripts{
						model.StaticScript{Value: `print(100)`},
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `1234`,
							},
						},
					},
				},
				{
					CodeKey: model.CodeKey{
						ComponentId: `keboola.python-transformation-v2`,
					},
					Name:          `Code 2`,
					PathInProject: model.NewPathInProject(`branch/transformation/blocks/block-1`, `code-2`),
					Scripts: model.Scripts{
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `5678`,
							},
						},
						model.LinkScript{
							Target: model.ConfigRowKey{
								BranchId:    123,
								ComponentId: `keboola.shared-code`,
								ConfigId:    `456`,
								Id:          `1234`,
							},
						},
					},
				},
			},
		},
	}, transformation.Local.Transformation.Blocks)

	// Values from content are converted to struct
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: sharedCodeRowsKeys}, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodePath(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)
	fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createLocalTranWithSharedCode(t, context)
	transformation.Local.Content.Set(model.SharedCodePathContentKey, `missing`) // <<<<<<<<<<<

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	expectedErr := `
missing shared code "branch/missing":
  - referenced from config "branch:123/component:keboola.python-transformation-v2/config:789"
`
	err := mapperInst.OnLocalChange(changes)
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logs.AllMessages())

	// Link to shared code is not set
	assert.Nil(t, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}

func TestLocalLoadTranWithSharedCode_InvalidSharedCodeRowPath(t *testing.T) {
	t.Parallel()
	mapperInst, context, logs := createMapper(t)
	sharedCodeKey, sharedCodeRowsKeys := fixtures.CreateSharedCode(t, context.State, context.NamingRegistry)

	// Create transformation with shared code
	transformation := createLocalTranWithSharedCode(t, context)
	transformation.Local.Transformation.Blocks[0].Codes[1].Scripts[0] = model.StaticScript{Value: "# {{:codes/missing}}\n"} // <<<<<<<<<<<<

	// Invoke
	changes := model.NewLocalChanges()
	changes.AddLoaded(transformation)
	expectedErr := `
missing shared code "branch/_shared/keboola.python-transformation-v2/codes/missing":
  - referenced from "branch/transformation/blocks/block-1/code-2"
`
	err := mapperInst.OnLocalChange(changes)
	assert.Error(t, err)
	assert.Equal(t, strings.TrimSpace(expectedErr), err.Error())
	assert.Empty(t, logs.AllMessages())

	// Link to shared code is set, but without invalid row
	assert.Equal(t, &model.LinkToSharedCode{Config: sharedCodeKey, Rows: []model.ConfigRowKey{sharedCodeRowsKeys[0]}}, transformation.Local.Transformation.LinkToSharedCode)

	// Key from Content is deleted
	_, found := transformation.Local.Content.Get(model.SharedCodePathContentKey)
	assert.False(t, found)
}
