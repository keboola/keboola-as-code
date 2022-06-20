package create

import (
	"context"
	"testing"

	"github.com/keboola/go-client/pkg/storageapi"
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
	ctx := NewContext(context.Background(), sourceBranch, configs)

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
			Search:  model.BranchKey{Id: 123},
			Replace: model.BranchKey{Id: 0},
		},
		{
			Search:  storageapi.BranchID(123),
			Replace: storageapi.BranchID(0),
		},
		{
			Search: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "123",
			},
			Replace: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				Id:          `<<~~func:ConfigId:["my-first-config"]~~>>`,
			},
		},
		{
			Search:  storageapi.ConfigID("123"),
			Replace: storageapi.ConfigID(`<<~~func:ConfigId:["my-first-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("123"),
			Replace: `<<~~func:ConfigId:["my-first-config"]~~>>`,
		},
		{
			Search: model.ConfigKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				Id:          "345",
			},
			Replace: model.ConfigKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				Id:          `<<~~func:ConfigId:["my-second-config"]~~>>`,
			},
		},
		{
			Search:  storageapi.ConfigID("345"),
			Replace: storageapi.ConfigID(`<<~~func:ConfigId:["my-second-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("345"),
			Replace: `<<~~func:ConfigId:["my-second-config"]~~>>`,
		},
		{
			Search: model.ConfigRowKey{
				BranchId:    sourceBranch.Id,
				ComponentId: "foo.bar",
				ConfigId:    "345",
				Id:          "789",
			},
			Replace: model.ConfigRowKey{
				BranchId:    0,
				ComponentId: "foo.bar",
				ConfigId:    `<<~~func:ConfigId:["my-second-config"]~~>>`,
				Id:          `<<~~func:ConfigRowId:["my-row"]~~>>`,
			},
		},
		{
			Search:  storageapi.RowID("789"),
			Replace: storageapi.RowID(`<<~~func:ConfigRowId:["my-row"]~~>>`),
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
