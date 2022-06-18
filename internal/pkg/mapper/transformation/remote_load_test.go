package transformation_test

import (
	"context"
	"testing"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestLoadRemoteTransformation(t *testing.T) {
	t.Parallel()
	state, d := createStateWithMapper(t)
	logger := d.DebugLogger()

	configState := createTestFixtures(t, "keboola.snowflake-transformation")

	// Api representation
	configInApi := `
{
  "parameters": {
    "blocks": [
      {
        "name": "block-1",
        "codes": [
          {
            "name": "code-1",
            "script": [
              "SELECT 1"
            ]
          },
          {
            "name": "code-2",
            "script": [
              "SELECT 1;",
              "SELECT 2;"
            ]
          }
        ]
      },
      {
        "name": "block-2",
        "codes": [
          {
            "name": "code-3",
            "script": [
              "SELECT 3"
            ]
          }
        ]
      }
    ]
  }
}
`

	// Load
	object := &model.Config{
		ConfigKey: configState.ConfigKey,
		Content:   orderedmap.New(),
	}
	json.MustDecodeString(configInApi, object.Content)
	recipe := model.NewRemoteLoadRecipe(configState.ConfigManifest, object)
	assert.NoError(t, state.Mapper().MapAfterRemoteLoad(context.Background(), recipe))
	assert.Empty(t, logger.WarnAndErrorMessages())

	// Internal representation
	expected := []*model.Block{
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       0,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`001-block-1`,
			),
			Name: "block-1",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`001-code-1`,
					),
					Name:         "code-1",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1"},
					},
				},
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  0,
						Index:       1,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/001-block-1`,
						`002-code-2`,
					),
					Name:         "code-2",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 1;"},
						model.StaticScript{Value: "SELECT 2;"},
					},
				},
			},
		},
		{
			BlockKey: model.BlockKey{
				BranchId:    123,
				ComponentId: "keboola.snowflake-transformation",
				ConfigId:    `456`,
				Index:       1,
			},
			AbsPath: model.NewAbsPath(
				`branch/config/blocks`,
				`002-block-2`,
			),
			Name: "block-2",
			Codes: model.Codes{
				{
					CodeKey: model.CodeKey{
						BranchId:    123,
						ComponentId: "keboola.snowflake-transformation",
						ConfigId:    `456`,
						BlockIndex:  1,
						Index:       0,
					},
					AbsPath: model.NewAbsPath(
						`branch/config/blocks/002-block-2`,
						`001-code-3`,
					),
					Name:         "code-3",
					CodeFileName: `code.sql`,
					Scripts: model.Scripts{
						model.StaticScript{Value: "SELECT 3"},
					},
				},
			},
		},
	}

	// In internal object are blocks in Blocks field, not in Content
	assert.Equal(t, `{"parameters":{}}`, json.MustEncodeString(object.Content, false))
	assert.Equal(t, expected, object.Transformation.Blocks)
}
