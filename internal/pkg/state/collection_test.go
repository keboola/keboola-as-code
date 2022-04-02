package state

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/sort"
)

func TestCollection_New(t *testing.T) {
	t.Parallel()
	c := NewCollection(sort.NewIdSorter())
	assert.NotNil(t, c)
}

func TestCollection_Add(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	assert.NoError(t, c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{
			ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`},
			ConfigRowId: `1000`,
		},
		Name: "Config Row 1000",
	}))
	assert.Len(t, c.All(), 7)
}

func TestCollection_Add_ParentNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{
			ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `999`},
			ConfigRowId: `1`,
		},
		Name: "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, "parent config \"branch:123/component:keboola.foo/config:999\" not found:\n  - referenced from config row \"branch:123/component:keboola.foo/config:999/row:1\"", err.Error())
}

func TestCollection_Add_AlreadyExists(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{
			ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`},
			ConfigRowId: `12`,
		},
		Name: "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, `config row "branch:123/component:keboola.foo/config:678/row:12" already exists`, err.Error())
}

func TestCollection_AddOrReplace_Add(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	// Row is NOT present
	rowKey := ConfigRowKey{
		ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`},
		ConfigRowId: `1000`,
	}
	_, found := c.Get(rowKey)
	assert.False(t, found)

	// Add
	assert.NoError(t, c.AddOrReplace(&ConfigRow{
		ConfigRowKey: rowKey,
		Name:         "New Config Row",
	}))

	// Row is added
	assert.Len(t, c.All(), 7)
	row, found := c.Get(rowKey)
	assert.True(t, found)
	assert.Equal(t, "New Config Row", row.(*ConfigRow).Name)
}

func TestCollection_AddOrReplace_Replace(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	// Row is already present
	rowKey := ConfigRowKey{
		ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`},
		ConfigRowId: `12`,
	}
	_, found := c.Get(rowKey)
	assert.True(t, found)

	// Replace
	assert.NoError(t, c.AddOrReplace(&ConfigRow{
		ConfigRowKey: rowKey,
		Name:         "Replaced Config Row",
	}))

	// Row is replaced
	assert.Len(t, c.All(), 6)
	row, found := c.Get(rowKey)
	assert.True(t, found)
	assert.Equal(t, "Replaced Config Row", row.(*ConfigRow).Name)
}

func TestCollection_AddOrReplace_ParentNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.AddOrReplace(&ConfigRow{
		ConfigRowKey: ConfigRowKey{
			ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `999`},
			ConfigRowId: `1`,
		},
		Name: "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, "parent config \"branch:123/component:keboola.foo/config:999\" not found:\n  - referenced from config row \"branch:123/component:keboola.foo/config:999/row:1\"", err.Error())
}

func TestCollection_Remove(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	rowKey := ConfigRowKey{
		ConfigKey:   ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`},
		ConfigRowId: `34`,
	}
	c.Remove(rowKey)
	_, found := c.Get(rowKey)
	assert.False(t, found)
	assert.Len(t, c.All(), 5)
}

func TestCollection_Remove_Cascade(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	c.Remove(BranchKey{BranchId: 123})
	assert.Len(t, c.All(), 1)
}

func TestCollection_Get(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{BranchId: 567})
	assert.NotNil(t, state)
	assert.True(t, found)
}

func TestCollection_GetWithChildren(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)

	// Fixtures
	branch1Key := BranchKey{BranchId: 123}
	branch2Key := BranchKey{BranchId: 567}
	config1Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `345`}
	config2Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `678`}
	configRow1Key := ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `12`}
	configRow2Key := ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `34`}
	branch1 := c.MustGet(branch1Key)
	branch2 := c.MustGet(branch2Key)
	config1 := c.MustGet(config1Key)
	config2 := c.MustGet(config2Key)
	configRow1 := c.MustGet(configRow1Key)
	configRow2 := c.MustGet(configRow2Key)

	// Add transformation config
	config3Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `999`}
	config3 := &Config{ConfigKey: config3Key}
	transformationKey := TransformationKey{ConfigKey: config3Key}
	transformation := &Transformation{TransformationKey: transformationKey}
	block := &Block{BlockKey: BlockKey{TransformationKey: transformationKey}}
	c.MustAdd(config3, transformation, block)

	// Missing
	_, found := c.GetWithChildren(BranchKey{BranchId: 999})
	assert.False(t, found)

	// Object without children - branch
	result, found := c.GetWithChildren(branch2Key)
	assert.True(t, found)
	assert.Equal(t, &ObjectWithChildren{
		Object:   branch2,
		Children: map[Kind][]*ObjectWithChildren{},
	}, result)

	// Object without children - config
	result, found = c.GetWithChildren(config1Key)
	assert.True(t, found)
	assert.Equal(t, &ObjectWithChildren{
		Object:   config1,
		Children: map[Kind][]*ObjectWithChildren{},
	}, result)

	// Object with children - branch
	result, found = c.GetWithChildren(branch1Key)
	assert.True(t, found)
	assert.Equal(t, &ObjectWithChildren{
		// Branch
		Object: branch1,
		Children: map[Kind][]*ObjectWithChildren{
			ConfigKind: {
				// Config 1
				{
					Object:   config1,
					Children: map[Kind][]*ObjectWithChildren{},
				},
				// Config 2 - with rows
				{
					Object: config2,
					Children: map[Kind][]*ObjectWithChildren{
						ConfigRowKind: {
							{
								Object:   configRow1,
								Children: map[Kind][]*ObjectWithChildren{},
							},
							{
								Object:   configRow2,
								Children: map[Kind][]*ObjectWithChildren{},
							},
						},
					},
				},
				// Config 3 - with transformation
				{
					Object: config3,
					Children: map[Kind][]*ObjectWithChildren{
						TransformationKind: {
							// Transformation
							{
								Object: transformation,
								Children: map[Kind][]*ObjectWithChildren{
									// Transformation block
									BlockKind: {
										{
											Object:   block,
											Children: map[Kind][]*ObjectWithChildren{},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}, result)

	// Object with children - config
	result, found = c.GetWithChildren(config2Key)
	assert.True(t, found)
	assert.Equal(t, &ObjectWithChildren{
		Object: config2,
		Children: map[Kind][]*ObjectWithChildren{
			ConfigRowKind: {
				{
					Object:   configRow1,
					Children: map[Kind][]*ObjectWithChildren{},
				},
				{
					Object:   configRow2,
					Children: map[Kind][]*ObjectWithChildren{},
				},
			},
		},
	}, result)

}

func TestCollection_Get_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{BranchId: 111})
	assert.Nil(t, state)
	assert.False(t, found)
}

func TestCollection_MustGet(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Equal(t, "Foo Bar Branch", c.MustGet(BranchKey{BranchId: 567}).(*Branch).Name)
}

func TestCollection_MustGet_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.PanicsWithError(t, `branch "branch:111" not found`, func() {
		c.MustGet(BranchKey{BranchId: 111})
	})
}

func TestCollection_All(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)
}

func TestCollection_AllWithChildren(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)

	// Fixtures
	branch1Key := BranchKey{BranchId: 123}
	branch2Key := BranchKey{BranchId: 567}
	config1Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `345`}
	config2Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `678`}
	configRow1Key := ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `12`}
	configRow2Key := ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `34`}
	branch1 := c.MustGet(branch1Key)
	branch2 := c.MustGet(branch2Key)
	config1 := c.MustGet(config1Key)
	config2 := c.MustGet(config2Key)
	configRow1 := c.MustGet(configRow1Key)
	configRow2 := c.MustGet(configRow2Key)

	objects := c.AllWithChildren()
	assert.Equal(t, []*ObjectWithChildren{
		// Branch 1
		{
			Object:   branch1,
			Children: map[Kind][]*ObjectWithChildren{},
		},
		// Branch 2
		{
			Object:   branch2,
			Children: map[Kind][]*ObjectWithChildren{},
		},
		// Config 1
		{
			Object:   config1,
			Children: map[Kind][]*ObjectWithChildren{},
		},
		// Config 2 - with rows
		{
			Object:   config2,
			Children: map[Kind][]*ObjectWithChildren{},
		},
		// Config row 1
		{
			Object:   configRow1,
			Children: map[Kind][]*ObjectWithChildren{},
		},
		// Config row 2
		{
			Object:   configRow2,
			Children: map[Kind][]*ObjectWithChildren{},
		},
	}, objects)
}

func TestCollection_Branches(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.Branches(), 2)
}

func TestCollection_Configs(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.Configs(), 2)
}

func TestCollection_ConfigsFrom(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigsFrom(BranchKey{BranchId: 123}), 2)
	assert.Len(t, c.ConfigsFrom(BranchKey{BranchId: 567}), 0)
	assert.Len(t, c.ConfigsFrom(BranchKey{BranchId: 111}), 0)
}

func TestCollection_ConfigsWithRowsFrom(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)

	configs := c.ConfigsWithRowsFrom(BranchKey{BranchId: 123})
	assert.Len(t, configs, 2)
	assert.Len(t, configs[0].Rows, 0)
	assert.Len(t, configs[1].Rows, 2)

	assert.Len(t, c.ConfigsFrom(BranchKey{BranchId: 567}), 0)
	assert.Len(t, c.ConfigsFrom(BranchKey{BranchId: 111}), 0)
}

func TestCollection_ConfigRows(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigRows(), 2)
}

func TestCollection_ConfigRowsFrom(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `678`}), 2)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `345`}), 0)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchKey: BranchKey{BranchId: 123}, ComponentId: "keboola.foo", ConfigId: `111`}), 0)
}

func newTestCollection(t *testing.T) Objects {
	t.Helper()
	collection := NewCollection(sort.NewPathSorter(naming.NewRegistry()))

	// Branch 1
	branch1Key := BranchKey{BranchId: 123}
	assert.NoError(t, collection.Add(&Branch{
		BranchKey: branch1Key,
		Name:      "Main",
		IsDefault: true,
	}))

	// Branch 2
	branch2Key := BranchKey{BranchId: 567}
	assert.NoError(t, collection.Add(&Branch{
		BranchKey: branch2Key,
		Name:      "Foo Bar Branch",
		IsDefault: false,
	}))

	// Config 1
	config1Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `345`}
	assert.NoError(t, collection.Add(&Config{
		ConfigKey: config1Key,
		Name:      "Config 1",
	}))

	// Config 2
	config2Key := ConfigKey{BranchKey: branch1Key, ComponentId: "keboola.foo", ConfigId: `678`}
	assert.NoError(t, collection.Add(&Config{
		ConfigKey: config2Key,
		Name:      "Config 2",
	}))

	// Config Row 1
	assert.NoError(t, collection.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `12`},
		Name:         "Config Row 1",
	}))

	// Config Row 2
	assert.NoError(t, collection.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{ConfigKey: config2Key, ConfigRowId: `34`},
		Name:         "Config Row 2",
	}))

	return collection
}
