package replacekeys

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestReplaceValues(t *testing.T) {
	t.Parallel()
	keys := Keys{
		{
			Old: model.BranchKey{
				Id: 123,
			},
			New: model.BranchKey{
				Id: 0,
			},
		},
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`,
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`,
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `45`,
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `67`,
			},
		},
	}
	replacements, err := keys.values()
	assert.NoError(t, err)
	assert.Equal(t, values{
		value{
			Old: model.BranchKey{Id: 123},
			New: model.BranchKey{Id: 0},
		},
		value{
			Old: model.BranchId(123),
			New: model.BranchId(0),
		},
		value{
			Old: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "12"},
			New: model.ConfigKey{BranchId: 1, ComponentId: "foo.bar", Id: "23"},
		},
		value{
			Old: model.ConfigId("12"),
			New: model.ConfigId("23"),
		},
		value{
			Old: subString("12"),
			New: "23",
		},
		value{
			Old: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "12", Id: "45"},
			New: model.ConfigRowKey{BranchId: 1, ComponentId: "foo.bar", ConfigId: "23", Id: "67"},
		},
		value{
			Old: model.RowId("45"),
			New: model.RowId("67"),
		},
		value{
			Old: subString("45"),
			New: "67",
		},
	}, replacements)
}

func TestReplaceValues_DuplicateOld(t *testing.T) {
	t.Parallel()
	keys := Keys{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`, // <<<<<<<<<<<<<
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`,
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `12`, // <<<<<<<<<<<<<
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `67`,
			},
		},
	}
	_, err := keys.values()
	assert.Error(t, err)
	assert.Equal(t, `the old ID "12" is defined 2x`, err.Error())
}

func TestReplaceValues_DuplicateNew(t *testing.T) {
	t.Parallel()
	keys := Keys{
		{
			Old: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `12`,
			},
			New: model.ConfigKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				Id:          `23`, // <<<<<<<<<<<<<
			},
		},
		{
			Old: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `12`,
				Id:          `45`,
			},
			New: model.ConfigRowKey{
				BranchId:    1,
				ComponentId: `foo.bar`,
				ConfigId:    `23`,
				Id:          `23`, // <<<<<<<<<<<<<
			},
		},
	}
	_, err := keys.values()
	assert.Error(t, err)
	assert.Equal(t, `the new ID "23" is defined 2x`, err.Error())
}

func TestReplaceKeys(t *testing.T) {
	t.Parallel()
	keys := Keys{
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

	values, err := keys.values()
	assert.NoError(t, err)
	assert.Equal(t, expected, replaceValues(values, input))
}

func TestReplaceSubString(t *testing.T) {
	t.Parallel()

	// Not found
	s := subString(`foo123`)
	out, found := s.replace(`bar`, `replaced`)
	assert.Equal(t, "", out)
	assert.False(t, found)

	// Full match
	s = subString(`foo123`)
	out, found = s.replace(`foo123`, `replaced`)
	assert.Equal(t, "replaced", out)
	assert.True(t, found)

	// At start
	s = subString(`foo123`)
	out, found = s.replace(`foo123-abc`, `replaced`)
	assert.Equal(t, "replaced-abc", out)
	assert.True(t, found)

	// At end
	s = subString(`foo123`)
	out, found = s.replace(`abc-foo123`, `replaced`)
	assert.Equal(t, "abc-replaced", out)
	assert.True(t, found)

	// Multiple matches
	s = subString(`foo123`)
	out, found = s.replace(`foo123-foo123-abc-foo123-def-foo123`, `replaced`)
	assert.Equal(t, "replaced-foo123-abc-replaced-def-replaced", out)
	assert.True(t, found)
}
