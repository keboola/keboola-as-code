package replacevalues

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestValues_AddKey(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.BranchKey{
			Id: 123,
		},
		model.BranchKey{
			Id: 0,
		},
	)
	replacements.AddKey(
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `12`,
		},
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `23`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `12`,
			Id:          `45`,
		},
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `23`,
			Id:          `67`,
		},
	)
	assert.Equal(t, []Value{
		{
			Search:  model.BranchKey{Id: 123},
			Replace: model.BranchKey{Id: 0},
		},
		{
			Search:  model.BranchId(123),
			Replace: model.BranchId(0),
		},
		{
			Search:  model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "12"},
			Replace: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "23"},
		},
		{
			Search:  model.ConfigId("12"),
			Replace: model.ConfigId("23"),
		},
		{
			Search:  SubString("12"),
			Replace: "23",
		},
		{
			Search:  model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "12", Id: "45"},
			Replace: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "23", Id: "67"},
		},
		{
			Search:  model.RowId("45"),
			Replace: model.RowId("67"),
		},
		{
			Search:  SubString("45"),
			Replace: "67",
		},
	}, replacements.values)
}

func TestValues_AddId(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddId(model.ConfigId("old1"), model.ConfigId("new1"))
	replacements.AddId(model.RowId("old2"), model.RowId("new2"))

	assert.Equal(t, []Value{
		{
			Search:  model.ConfigId("old1"),
			Replace: model.ConfigId("new1"),
		},
		{
			Search:  SubString("old1"),
			Replace: "new1",
		},
		{
			Search:  model.RowId("old2"),
			Replace: model.RowId("new2"),
		},
		{
			Search:  SubString("old2"),
			Replace: "new2",
		},
	}, replacements.values)
}

func TestValues_Validate_DuplicateOld(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `12`, // <<<<<<<<<<<<<
		},
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `23`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `12`,
			Id:          `12`, // <<<<<<<<<<<<<
		},
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `23`,
			Id:          `67`,
		},
	)

	err := replacements.validate()
	assert.Error(t, err)
	assert.Equal(t, `the old ID "12" is defined 2x`, err.Error())
}

func TestValues_Validate_DuplicateNew(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `12`,
		},
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `23`, // <<<<<<<<<<<<<
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `12`,
			Id:          `45`,
		},
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `23`,
			Id:          `23`, // <<<<<<<<<<<<<
		},
	)

	err := replacements.validate()
	assert.Error(t, err)
	assert.Equal(t, `the new ID "23" is defined 2x`, err.Error())
}

func TestValues_Replace(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `12`,
		},
		model.ConfigKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			Id:          `config-in-template`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `12`,
			Id:          `34`,
		},
		model.ConfigRowKey{
			BranchId:    1,
			ComponentId: `foo.bar`,
			ConfigId:    `config-in-template`,
			Id:          `row-in-template`,
		},
	)
	replacements.AddContentField(
		model.ConfigKey{BranchId: 1, ComponentId: `foo.bar`, Id: `12`},
		orderedmap.Key{orderedmap.MapStep("key1"), orderedmap.MapStep("key2")},
		"new value in config",
	)
	replacements.AddContentField(
		model.ConfigRowKey{BranchId: 1, ComponentId: `foo.bar`, ConfigId: `12`, Id: `56`},
		orderedmap.Key{orderedmap.MapStep("key3"), orderedmap.MapStep("key4")},
		"new value in row 56",
	)

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
					{Key: `some-row-id`, Value: model.RowId(`34`)},
					{Key: "key1", Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key2", Value: 123},
					})},
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "old value"},
						})},
					}),
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `12`,
						Id:          `56`,
					},
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "old value"},
						})},
					}),
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
					{Key: `some-row-id`, Value: model.RowId(`row-in-template`)},
					{Key: "key1", Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key2", Value: "new value in config"},
					})},
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
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "old value"},
						})},
					}),
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchId:    1,
						ComponentId: `foo.bar`,
						ConfigId:    `config-in-template`,
						Id:          `56`,
					},
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "new value in row 56"},
						})},
					}),
				},
			},
		},
	}

	replaced, err := replacements.Replace(input)
	assert.NoError(t, err)
	assert.Equal(t, expected, replaced)
}

func TestSubString_Replace(t *testing.T) {
	t.Parallel()

	// Not found
	s := SubString(`foo123`)
	out, found := s.replace(`bar`, `replaced`)
	assert.Equal(t, "", out)
	assert.False(t, found)

	// Full match
	s = SubString(`foo123`)
	out, found = s.replace(`foo123`, `replaced`)
	assert.Equal(t, "replaced", out)
	assert.True(t, found)

	// At start
	s = SubString(`foo123`)
	out, found = s.replace(`foo123-abc`, `replaced`)
	assert.Equal(t, "replaced-abc", out)
	assert.True(t, found)

	// At end
	s = SubString(`foo123`)
	out, found = s.replace(`abc-foo123`, `replaced`)
	assert.Equal(t, "abc-replaced", out)
	assert.True(t, found)

	// Multiple matches
	s = SubString(`foo123`)
	out, found = s.replace(`foo123-foo123-abc-foo123-def-foo123`, `replaced`)
	assert.Equal(t, "replaced-foo123-abc-replaced-def-replaced", out)
	assert.True(t, found)
}

func TestValues_AddContentField(t *testing.T) {
	t.Parallel()

	objectKey := model.ConfigKey{BranchId: 123, ComponentId: "foo.bar", Id: "123"}
	fieldPath := orderedmap.Key{orderedmap.MapStep("foo"), orderedmap.SliceStep(123)}

	replacements := NewValues()
	replacements.AddContentField(objectKey, fieldPath, "new value")

	assert.Equal(t, []Value{
		{
			Search: ContentField{
				objectKey: objectKey,
				fieldPath: fieldPath,
			},
			Replace: "new value",
		},
	}, replacements.values)
}
