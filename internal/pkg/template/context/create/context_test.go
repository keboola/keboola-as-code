package create

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacevalues"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestCreateContext(t *testing.T) {
	t.Parallel()

	sourceBranch := model.BranchKey{ID: 123}
	configs := []ConfigDef{
		{
			Key: model.ConfigKey{
				BranchID:    sourceBranch.ID,
				ComponentID: "foo.bar",
				ID:          "123",
			},
			TemplateID: "my-first-config",
		},
		{
			Key: model.ConfigKey{
				BranchID:    sourceBranch.ID,
				ComponentID: "foo.bar",
				ID:          "345",
			},
			TemplateID: "my-second-config",
			Rows: []ConfigRowDef{
				{
					Key: model.ConfigRowKey{
						BranchID:    sourceBranch.ID,
						ComponentID: "foo.bar",
						ConfigID:    "345",
						ID:          "789",
					},
					TemplateID: "my-row",
				},
			},
		},
	}
	ctx := NewContext(t.Context(), sourceBranch, configs)

	// Check remote filter
	expectedFilter := model.NoFilter()
	expectedFilter.SetAllowedKeys([]model.Key{
		model.BranchKey{ID: 123},
		model.ConfigKey{
			BranchID:    sourceBranch.ID,
			ComponentID: "foo.bar",
			ID:          "123",
		},
		model.ConfigKey{
			BranchID:    sourceBranch.ID,
			ComponentID: "foo.bar",
			ID:          "345",
		},
		model.ConfigRowKey{
			BranchID:    sourceBranch.ID,
			ComponentID: "foo.bar",
			ConfigID:    "345",
			ID:          "789",
		},
	})
	assert.Equal(t, expectedFilter, ctx.RemoteObjectsFilter())

	// Check replacements
	expectedReplacements := []replacevalues.Value{
		{
			Search:  model.BranchKey{ID: 123},
			Replace: model.BranchKey{ID: 0},
		},
		{
			Search:  keboola.BranchID(123),
			Replace: keboola.BranchID(0),
		},
		{
			Search: model.ConfigKey{
				BranchID:    sourceBranch.ID,
				ComponentID: "foo.bar",
				ID:          "123",
			},
			Replace: model.ConfigKey{
				BranchID:    0,
				ComponentID: "foo.bar",
				ID:          `<<~~func:ConfigId:["my-first-config"]~~>>`,
			},
		},
		{
			Search:  keboola.ConfigID("123"),
			Replace: keboola.ConfigID(`<<~~func:ConfigId:["my-first-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("123"),
			Replace: `<<~~func:ConfigId:["my-first-config"]~~>>`,
		},
		{
			Search: model.ConfigKey{
				BranchID:    sourceBranch.ID,
				ComponentID: "foo.bar",
				ID:          "345",
			},
			Replace: model.ConfigKey{
				BranchID:    0,
				ComponentID: "foo.bar",
				ID:          `<<~~func:ConfigId:["my-second-config"]~~>>`,
			},
		},
		{
			Search:  keboola.ConfigID("345"),
			Replace: keboola.ConfigID(`<<~~func:ConfigId:["my-second-config"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("345"),
			Replace: `<<~~func:ConfigId:["my-second-config"]~~>>`,
		},
		{
			Search: model.ConfigRowKey{
				BranchID:    sourceBranch.ID,
				ComponentID: "foo.bar",
				ConfigID:    "345",
				ID:          "789",
			},
			Replace: model.ConfigRowKey{
				BranchID:    0,
				ComponentID: "foo.bar",
				ConfigID:    `<<~~func:ConfigId:["my-second-config"]~~>>`,
				ID:          `<<~~func:ConfigRowId:["my-row"]~~>>`,
			},
		},
		{
			Search:  keboola.RowID("789"),
			Replace: keboola.RowID(`<<~~func:ConfigRowId:["my-row"]~~>>`),
		},
		{
			Search:  replacevalues.SubString("789"),
			Replace: `<<~~func:ConfigRowId:["my-row"]~~>>`,
		},
	}
	replacements, err := ctx.Replacements()
	require.NoError(t, err)
	assert.Equal(t, expectedReplacements, replacements.Values())
}
