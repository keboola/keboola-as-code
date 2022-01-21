package template

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestTemplate_ReplaceKeys(t *testing.T) {
	t.Parallel()
	replacement := KeysReplacement{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`,
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `config-in-template`,
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `34`,
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `config-in-template`,
				Id:          `row-in-template`,
			},
		},
	}

	// Project objects
	input := []model.Object{
		model.ConfigWithRows{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    1,
					ComponentId: `foo.bar`,
					Id:          `12`,
				},
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{
						Key:   `some-row-id`,
						Value: model.RowId(`34`),
					},
				}),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `12`,
						Id:          `34`,
					},
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `12`,
						Id:          `56`,
					},
				},
			},
		},
	}

	// Template objects
	expected := []model.Object{
		model.ConfigWithRows{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchId:    1,
					ComponentId: `foo.bar`,
					Id:          `config-in-template`,
				},
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{
						Key:   `some-row-id`,
						Value: model.RowId(`row-in-template`),
					},
				}),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `config-in-template`,
						Id:          `row-in-template`,
					},
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `config-in-template`,
						Id:          `56`,
					},
				},
			},
		},
	}

	tmpl := &template{}
	tmpl.objects = input
	assert.NoError(t, tmpl.ReplaceKeys(replacement))
	assert.Equal(t, expected, tmpl.objects)
}
