package replacevalues

import (
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func TestValues_AddKey(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.BranchKey{
			ID: 123,
		},
		model.BranchKey{
			ID: 0,
		},
	)
	replacements.AddKey(
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `12`,
		},
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `23`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `12`,
			ID:          `45`,
		},
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `23`,
			ID:          `67`,
		},
	)
	assert.Equal(t, []Value{
		{
			Search:  model.BranchKey{ID: 123},
			Replace: model.BranchKey{ID: 0},
		},
		{
			Search:  keboola.BranchID(123),
			Replace: keboola.BranchID(0),
		},
		{
			Search:  model.ConfigKey{BranchID: 1, ComponentID: "foo.bar", ID: "12"},
			Replace: model.ConfigKey{BranchID: 1, ComponentID: "foo.bar", ID: "23"},
		},
		{
			Search:  keboola.ConfigID("12"),
			Replace: keboola.ConfigID("23"),
		},
		{
			Search:  SubString("12"),
			Replace: "23",
		},
		{
			Search:  model.ConfigRowKey{BranchID: 1, ComponentID: "foo.bar", ConfigID: "12", ID: "45"},
			Replace: model.ConfigRowKey{BranchID: 1, ComponentID: "foo.bar", ConfigID: "23", ID: "67"},
		},
		{
			Search:  keboola.RowID("45"),
			Replace: keboola.RowID("67"),
		},
		{
			Search:  SubString("45"),
			Replace: "67",
		},
	}, replacements.values)
}

func TestValues_AddID(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddID(keboola.ConfigID("old1"), keboola.ConfigID("new1"))
	replacements.AddID(keboola.RowID("old2"), keboola.RowID("new2"))

	assert.Equal(t, []Value{
		{
			Search:  keboola.ConfigID("old1"),
			Replace: keboola.ConfigID("new1"),
		},
		{
			Search:  SubString("old1"),
			Replace: "new1",
		},
		{
			Search:  keboola.RowID("old2"),
			Replace: keboola.RowID("new2"),
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
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `12`, // <<<<<<<<<<<<<
		},
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `23`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `12`,
			ID:          `12`, // <<<<<<<<<<<<<
		},
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `23`,
			ID:          `67`,
		},
	)

	err := replacements.validate()
	require.Error(t, err)
	assert.Equal(t, `the old ID "12" is defined 2x`, err.Error())
}

func TestValues_Validate_DuplicateNew(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `12`,
		},
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `23`, // <<<<<<<<<<<<<
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `12`,
			ID:          `45`,
		},
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `23`,
			ID:          `23`, // <<<<<<<<<<<<<
		},
	)

	err := replacements.validate()
	require.Error(t, err)
	assert.Equal(t, `the new ID "23" is defined 2x`, err.Error())
}

func TestValues_Replace(t *testing.T) {
	t.Parallel()

	replacements := NewValues()
	replacements.AddKey(
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `12`,
		},
		model.ConfigKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ID:          `config-in-template`,
		},
	)
	replacements.AddKey(
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `12`,
			ID:          `34`,
		},
		model.ConfigRowKey{
			BranchID:    1,
			ComponentID: `foo.bar`,
			ConfigID:    `config-in-template`,
			ID:          `row-in-template`,
		},
	)
	replacements.AddContentField(
		model.ConfigKey{BranchID: 1, ComponentID: `foo.bar`, ID: `12`},
		orderedmap.Path{orderedmap.MapStep("key1"), orderedmap.MapStep("key2")},
		"new value in config",
	)
	replacements.AddContentField(
		model.ConfigRowKey{BranchID: 1, ComponentID: `foo.bar`, ConfigID: `12`, ID: `56`},
		orderedmap.Path{orderedmap.MapStep("key3"), orderedmap.MapStep("key4")},
		"new value in row 56",
	)

	// Project objects
	input := []model.Object{
		&model.ConfigWithRows{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    1,
					ComponentID: `foo.bar`,
					ID:          `12`,
				},
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `some-row-id`, Value: keboola.RowID(`34`)},
					{Key: "key1", Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key2", Value: 123},
					})},
				}),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchID:    1,
						ComponentID: `foo.bar`,
						ConfigID:    `12`,
						ID:          `34`,
					},
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "old value"},
						})},
					}),
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchID:    1,
						ComponentID: `foo.bar`,
						ConfigID:    `12`,
						ID:          `56`,
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
		&model.ConfigWithRows{
			Config: &model.Config{
				ConfigKey: model.ConfigKey{
					BranchID:    1,
					ComponentID: `foo.bar`,
					ID:          `config-in-template`,
				},
				Content: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: `some-row-id`, Value: keboola.RowID(`row-in-template`)},
					{Key: "key1", Value: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key2", Value: "new value in config"},
					})},
				}),
			},
			Rows: []*model.ConfigRow{
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchID:    1,
						ComponentID: `foo.bar`,
						ConfigID:    `config-in-template`,
						ID:          `row-in-template`,
					},
					Content: orderedmap.FromPairs([]orderedmap.Pair{
						{Key: "key3", Value: orderedmap.FromPairs([]orderedmap.Pair{
							{Key: "key4", Value: "old value"},
						})},
					}),
				},
				{
					ConfigRowKey: model.ConfigRowKey{
						BranchID:    1,
						ComponentID: `foo.bar`,
						ConfigID:    `config-in-template`,
						ID:          `56`,
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
	require.NoError(t, err)
	assert.Equal(t, expected, replaced)
}

func TestSubString_Replace(t *testing.T) {
	t.Parallel()

	// Not found
	s := SubString(`foo123`)
	out, found := s.replace(`bar`, `replaced`)
	assert.Empty(t, out)
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

	objectKey := model.ConfigKey{BranchID: 123, ComponentID: "foo.bar", ID: "123"}
	fieldPath := orderedmap.Path{orderedmap.MapStep("foo"), orderedmap.SliceStep(123)}

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
