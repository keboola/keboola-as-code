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
			Old: model.BranchKey{Id: 123},
			New: model.BranchKey{Id: 0},
		},
		{
			Old: model.BranchId(123),
			New: model.BranchId(0),
		},
		{
			Old: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "12"},
			New: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "23"},
		},
		{
			Old: model.ConfigId("12"),
			New: model.ConfigId("23"),
		},
		{
			Old: SubString("12"),
			New: "23",
		},
		{
			Old: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "12", Id: "45"},
			New: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "23", Id: "67"},
		},
		{
			Old: model.RowId("45"),
			New: model.RowId("67"),
		},
		{
			Old: SubString("45"),
			New: "67",
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
			Old: model.ConfigId("old1"),
			New: model.ConfigId("new1"),
		},
		{
			Old: SubString("old1"),
			New: "new1",
		},
		{
			Old: model.RowId("old2"),
			New: model.RowId("new2"),
		},
		{
			Old: SubString("old2"),
			New: "new2",
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
