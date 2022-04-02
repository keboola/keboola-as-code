package template

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template/replacevalues"
)

func TestCreateContext(t *testing.T) {
	t.Parallel()

	sourceBranch := model.BranchKey{BranchId: 123}
	configs := []ConfigDef{
		{
			Key: model.ConfigKey{
				BranchId:    sourceBranch.BranchId,
				ComponentId: "foo.bar",
				ConfigId:    "123",
			},
			TemplateId: "my-first-config",
		},
		{
			Key: model.ConfigKey{
				BranchId:    sourceBranch.BranchId,
				ComponentId: "foo.bar",
				ConfigId:    "345",
			},
			TemplateId: "my-second-config",
			Rows: []ConfigRowDef{
				{
					Key: model.ConfigRowKey{
						BranchId:    sourceBranch.BranchId,
						ComponentId: "foo.bar",
						ConfigId:    "345",
						ConfigRowId: "789",
					},
					TemplateId: "my-row",
				},
			},
		},
	}
	ctx := NewCreateContext(context.Background(), sourceBranch, configs)

	// Check remote filter
	expectedFilter := model.NoFilter()
	expectedFilter.SetAllowedKeys([]model.Key{
		model.BranchKey{BranchId: 123},
		model.ConfigKey{
			BranchId:    sourceBranch.BranchId,
			ComponentId: "foo.bar",
			ConfigId:    "123",
		},
		model.ConfigKey{
			BranchId:    sourceBranch.BranchId,
			ComponentId: "foo.bar",
			ConfigId:    "345",
		},
		model.ConfigRowKey{
			BranchId:    sourceBranch.BranchId,
			ComponentId: "foo.bar",
			ConfigId:    "345",
			ConfigRowId: "789",
		},
	})
	assert.Equal(t, expectedFilter, ctx.RemoteObjectsFilter())

	// Check replacements
	expectedReplacements := []replacevalues.Value{
		{
			Search:  model.BranchKey{BranchId: 123},
			Replace: model.BranchKey{BranchId: 0},
		},
		{
			Search:  model.BranchId(123),
			Replace: model.BranchId(0),
		},
		{
			Search: model.ConfigKey{
				BranchId:    sourceBranch.BranchId,
				ComponentId: "foo.bar",
				ConfigId:    "123",
			},
			Replace: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				ConfigId:    `<<~~func:ConfigId:["my-first-config"]~~>>`,
			},
		},
		{
			Search:  model.ConfigId("123"),
			Replace: model.ConfigId(`<<~~func:ConfigId:["my-first-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("123"),
			Replace: `<<~~func:ConfigId:["my-first-config"]~~>>`,
		},
		{
			Search: model.ConfigKey{
				BranchId:    sourceBranch.BranchId,
				ComponentId: "foo.bar",
				ConfigId:    "345",
			},
			Replace: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				ConfigId:    `<<~~func:ConfigId:["my-second-config"]~~>>`,
			},
		},
		{
			Search:  model.ConfigId("345"),
			Replace: model.ConfigId(`<<~~func:ConfigId:["my-second-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("345"),
			Replace: `<<~~func:ConfigId:["my-second-config"]~~>>`,
		},
		{
			Search: model.ConfigRowKey{
				BranchId:    sourceBranch.BranchId,
				ComponentId: "foo.bar",
				ConfigId:    "345",
				ConfigRowId: "789",
			},
			Replace: model.ConfigRowKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				ConfigId:    `<<~~func:ConfigId:["my-second-config"]~~>>`,
				ConfigRowId: `<<~~func:ConfigRowId:["my-row"]~~>>`,
			},
		},
		{
			Search:  model.ConfigRowId("789"),
			Replace: model.ConfigRowId(`<<~~func:ConfigRowId:["my-row"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("789"),
			Replace: `<<~~func:ConfigRowId:["my-row"]~~>>`,
		},
	}
	replacements, err := ctx.Replacements()
	assert.NoError(t, err)
	assert.Equal(t, expectedReplacements, replacements.Values())
}
