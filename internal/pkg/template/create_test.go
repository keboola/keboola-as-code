package template

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestCreateContext(t *testing.T) {
	t.Parallel()

	sourceBranch := model.BranchKey{Id: 123}
	configs := []ConfigDef{
		{
			Key: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "123",
			},
			TemplateId: "my-first-config",
		},
		{
			Key: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "345",
			},
			TemplateId: "my-second-config",
			Rows: []ConfigRowDef{
				{
					Key: model.ConfigRowKey{
						BranchId:    sourceBranch.Id,
						ComponentId: "foo.bar",
						ConfigId:    "345",
						Id:          "789",
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
		model.BranchKey{Id: 123},
		model.ConfigKey{
			BranchId:    sourceBranch.Id,
			ComponentId: "foo.bar",
			Id:          "123",
		},
		model.ConfigKey{
			BranchId:    sourceBranch.Id,
			ComponentId: "foo.bar",
			Id:          "345",
		},
		model.ConfigRowKey{
			BranchId:    sourceBranch.Id,
			ComponentId: "foo.bar",
			ConfigId:    "345",
			Id:          "789",
		},
	})
	assert.Equal(t, expectedFilter, ctx.RemoteObjectsFilter())

	// Check replacements
	expectedReplacements := []replacevalues.Value{
		{
			Old: model.BranchKey{Id: 123},
			New: model.BranchKey{Id: 0},
		},
		{
			Old: model.BranchId(123),
			New: model.BranchId(0),
		},
		{
			Old: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "123",
			},
			New: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				Id:          `<<~~func:ConfigId:["my-first-config"]~~>>`,
			},
		},
		{
			Old: model.ConfigId("123"),
			New: model.ConfigId(`<<~~func:ConfigId:["my-first-config"]~~>>`),
		},
		{
			Old: replacevalues.SubString("123"),
			New: `<<~~func:ConfigId:["my-first-config"]~~>>`,
		},
		{
			Old: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "345",
			},
			New: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				Id:          `<<~~func:ConfigId:["my-second-config"]~~>>`,
			},
		},
		{
			Old: model.ConfigId("345"),
			New: model.ConfigId(`<<~~func:ConfigId:["my-second-config"]~~>>`),
		},
		{
			Old: replacevalues.SubString("345"),
			New: `<<~~func:ConfigId:["my-second-config"]~~>>`,
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				ConfigId:    "345",
				Id:          "789",
			},
			New: model.ConfigRowKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				ConfigId:    `<<~~func:ConfigId:["my-second-config"]~~>>`,
				Id:          `<<~~func:ConfigRowId:["my-row"]~~>>`,
			},
		},
		{
			Old: model.RowId("789"),
			New: model.RowId(`<<~~func:ConfigRowId:["my-row"]~~>>`),
		},
		{
			Old: replacevalues.SubString("789"),
			New: `<<~~func:ConfigRowId:["my-row"]~~>>`,
		},
	}
	replacements, err := ctx.Replacements()
	assert.NoError(t, err)
	assert.Equal(t, expectedReplacements, replacements.Values())
}
