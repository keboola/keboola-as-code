package object

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
)

func TestCollection_New(t *testing.T) {
	t.Parallel()
	c := New(NewIdSorter())
	assert.NotNil(t, c)
}

func TestCollection_Add(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	assert.NoError(t, c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `1000`},
		Name:         "Config Row 1000",
	}))
	assert.Len(t, c.All(), 7)
}

func TestCollection_Add_ParentNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `999`, Id: `1`},
		Name:         "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, "", err.Error())
}

func TestCollection_Add_AlreadyExists(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `12`},
		Name:         "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, "", err.Error())
}

func TestCollection_AddOrReplace_Add(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	// Row is NOT present
	rowKey := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `1000`}
	_, found := c.Get(rowKey)
	assert.False(t, found)

	// Add
	assert.NoError(t, c.Add(&ConfigRow{
		Name: "New Config Row",
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
	rowKey := ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `12`}
	_, found := c.Get(rowKey)
	assert.True(t, found)

	// Replace
	assert.NoError(t, c.Add(&ConfigRow{
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
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `999`, Id: `1`},
		Name:         "Config Row",
	})
	assert.Error(t, err)
	assert.Equal(t, "", err.Error())
}

func TestCollection_All(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)
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

func TestCollection_ConfigRows(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigRows(), 2)
}

func TestCollection_ConfigsFrom(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigsFrom(BranchKey{Id: 123}), 2)
	assert.Len(t, c.ConfigsFrom(BranchKey{Id: 567}), 0)
	assert.Len(t, c.ConfigsFrom(BranchKey{Id: 111}), 0)
}

func TestCollection_ConfigRowsFrom(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `678`}), 2)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `345`}), 0)
	assert.Len(t, c.ConfigRowsFrom(ConfigKey{BranchId: 123, ComponentId: "keboola.bar", Id: `111`}), 0)
}

func TestCollection_Get(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{Id: 567})
	assert.NotNil(t, state)
	assert.True(t, found)
}

func TestCollection_GetNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{Id: 111})
	assert.Nil(t, state)
	assert.False(t, found)
}

func TestCollection_MustGet(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Equal(t, "Foo Bar Branch", c.MustGet(BranchKey{Id: 567}).ObjectName())
}

func TestCollection_MustGetNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.PanicsWithError(t, `branch "111" not found`, func() {
		c.MustGet(BranchKey{Id: 111})
	})
}

func newTestCollection(t *testing.T) Objects {
	t.Helper()
	collection := New(NewPathSorter(naming.NewRegistry()))

	// Branch 1
	assert.NoError(t, collection.Add(&Branch{
		BranchKey: BranchKey{Id: 123},
		Name:      "Main",
		IsDefault: true,
	}))

	// Branch 2
	assert.NoError(t, collection.Add(&Branch{
		BranchKey: BranchKey{Id: 567},
		Name:      "Foo Bar Branch",
		IsDefault: false,
	}))

	// Config 1
	assert.NoError(t, collection.Add(&Config{
		ConfigKey: ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: `345`},
		Name:      "Config 1",
	}))

	// Config 2
	assert.NoError(t, collection.Add(&Config{
		ConfigKey: ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: `678`},
		Name:      "Config 2",
	}))

	// Config Row 1
	assert.NoError(t, collection.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `12`},
		Name:         "Config Row 1",
	}))

	// Config Row 2
	assert.NoError(t, collection.Add(&ConfigRow{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.bar", ConfigId: `678`, Id: `34`},
		Name:         "Config Row 2",
	}))

	return collection
}
