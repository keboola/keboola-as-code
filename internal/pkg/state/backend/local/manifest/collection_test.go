package manifest_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	. "github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
)

func TestCollection_New(t *testing.T) {
	t.Parallel()
	c := NewCollection(context.Background(), naming.NewRegistry(), state.NewIdSorter())
	assert.NotNil(t, c)
	assert.False(t, c.IsChanged())
}

func TestCollection_Set(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	assert.NoError(t, c.Set([]ObjectManifest{
		&BranchManifest{
			BranchKey: BranchKey{Id: 1},
			AbsPath:   NewAbsPath("", "branch-1"),
		},
		&BranchManifest{
			BranchKey: BranchKey{Id: 2},
			AbsPath:   NewAbsPath("", "branch-2"),
		},
	}))
	assert.Len(t, c.All(), 2)
	assert.False(t, c.IsChanged())
}

func TestCollection_Add(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	assert.NoError(t, c.Add(&ConfigRowManifest{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "1000"},
		AbsPath:      NewAbsPath("main/config-1", "row-1000"),
	}))
	assert.Len(t, c.All(), 7)
	assert.True(t, c.IsChanged())
}

func TestCollection_Add_ResolveParentPath_1(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	key := BranchKey{Id: 789}
	assert.NoError(t, c.Add(&BranchManifest{
		BranchKey: key,
		AbsPath:   AbsPath{RelPath: "my-branch"},
	}))

	assert.Len(t, c.All(), 7)
	assert.True(t, c.IsChanged())

	v, found := c.Get(key)
	assert.True(t, found)
	assert.True(t, v.Path().IsSet())
	assert.Equal(t, "", v.Path().ParentPath())
	assert.Equal(t, "my-branch", v.Path().RelativePath())
}

func TestCollection_Add_ResolveParentPath_2(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	key := ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "1000"}
	assert.NoError(t, c.Add(&ConfigRowManifest{
		ConfigRowKey: key,
		AbsPath:      AbsPath{RelPath: "row-1000"},
	}))

	assert.Len(t, c.All(), 7)
	assert.True(t, c.IsChanged())

	v, found := c.Get(key)
	assert.True(t, found)
	assert.True(t, v.Path().IsSet())
	assert.Equal(t, "main/config-2", v.Path().ParentPath())
	assert.Equal(t, "row-1000", v.Path().RelativePath())
}

func TestCollection_Add_AlreadyExists(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	assert.NoError(t, c.Add(&ConfigRowManifest{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "12"},
		AbsPath:      NewAbsPath("main/config-1", "row-1"),
	}))
	assert.True(t, c.IsChanged())
}

func TestCollection_Add_ParentNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(&ConfigRowManifest{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "999", Id: "1"},
	})

	expected := `
config "branch:123/component:keboola.foo/config:999" not found:
  - referenced as a parent of config row "branch:123/component:keboola.foo/config:999/row:1"
`

	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
	assert.False(t, c.IsChanged())
}

func TestCollection_Add_CyclicRelations_1(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(
		// Cyclic relation 1 -> 2 -> 1
		&ConfigManifest{
			ConfigKey: ConfigKey{BranchId: 123, ComponentId: VariablesComponentId, Id: "1"},
			Relations: Relations{&VariablesForRelation{
				ComponentId: VariablesComponentId,
				ConfigId:    "2",
			}},
			AbsPath: NewAbsPath("", "variables-1"),
		},
		&ConfigManifest{
			ConfigKey: ConfigKey{BranchId: 123, ComponentId: VariablesComponentId, Id: "2"},
			Relations: Relations{&VariablesForRelation{
				ComponentId: VariablesComponentId,
				ConfigId:    "1",
			}},
			AbsPath: NewAbsPath("", "variables-2"),
		},
	)

	expected := `
a cyclic relation found:
  - config "branch:123/component:keboola.variables/config:1" is child of
  - config "branch:123/component:keboola.variables/config:2" is child of
  - config "branch:123/component:keboola.variables/config:1"
`

	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
	assert.False(t, c.IsChanged())
}

func TestCollection_Add_CyclicRelations_2(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	err := c.Add(
		// Cyclic relation 1 -> 2 -> 3 -> 1
		&ConfigManifest{
			ConfigKey: ConfigKey{BranchId: 123, ComponentId: VariablesComponentId, Id: "1"},
			Relations: Relations{&VariablesForRelation{
				ComponentId: VariablesComponentId,
				ConfigId:    "2",
			}},
			AbsPath: NewAbsPath("", "variables-1"),
		},
		&ConfigManifest{
			ConfigKey: ConfigKey{BranchId: 123, ComponentId: VariablesComponentId, Id: "2"},
			Relations: Relations{&VariablesForRelation{
				ComponentId: VariablesComponentId,
				ConfigId:    "3",
			}},
			AbsPath: NewAbsPath("", "variables-2"),
		},
		&ConfigManifest{
			ConfigKey: ConfigKey{BranchId: 123, ComponentId: VariablesComponentId, Id: "3"},
			Relations: Relations{&VariablesForRelation{
				ComponentId: VariablesComponentId,
				ConfigId:    "1",
			}},
			AbsPath: NewAbsPath("", "variables-3"),
		},
	)

	expected := `
a cyclic relation found:
  - config "branch:123/component:keboola.variables/config:1" is child of
  - config "branch:123/component:keboola.variables/config:2" is child of
  - config "branch:123/component:keboola.variables/config:3" is child of
  - config "branch:123/component:keboola.variables/config:1"
`

	assert.Error(t, err)
	assert.Equal(t, strings.Trim(expected, "\n"), err.Error())
	assert.False(t, c.IsChanged())
}

func TestCollection_Remove(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	c.Remove(ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "34"})
	assert.Len(t, c.All(), 5)
	assert.True(t, c.IsChanged())
}

func TestCollection_Remove_Cascade(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)

	c.Remove(BranchKey{Id: 123})
	assert.Len(t, c.All(), 1)
	assert.True(t, c.IsChanged())
}

func TestCollection_Get(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{Id: 567})
	assert.NotNil(t, state)
	assert.True(t, found)
}

func TestCollection_Get_NotFound(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	state, found := c.Get(BranchKey{Id: 111})
	assert.Nil(t, state)
	assert.False(t, found)
	assert.False(t, c.IsChanged())
}

func TestCollection_All(t *testing.T) {
	t.Parallel()
	c := newTestCollection(t)
	assert.Len(t, c.All(), 6)
	assert.False(t, c.IsChanged())
}

func newTestCollection(t *testing.T) *Collection {
	t.Helper()
	namingRegistry := naming.NewRegistry()
	collection := NewCollection(context.Background(), namingRegistry, state.NewPathSorter(namingRegistry))

	// Branch 1
	assert.NoError(t, collection.Add(&BranchManifest{
		BranchKey: BranchKey{Id: 123},
		AbsPath:   NewAbsPath("", "main"),
	}))

	// Branch 2
	assert.NoError(t, collection.Add(&BranchManifest{
		BranchKey: BranchKey{Id: 567},
		AbsPath:   NewAbsPath("", "foo-bar"),
	}))

	// Config 1
	assert.NoError(t, collection.Add(&ConfigManifest{
		ConfigKey: ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: "345"},
		AbsPath:   NewAbsPath("main", "config-1"),
	}))

	// Config 2
	assert.NoError(t, collection.Add(&ConfigManifest{
		ConfigKey: ConfigKey{BranchId: 123, ComponentId: "keboola.foo", Id: "678"},
		AbsPath:   NewAbsPath("main", "config-2"),
	}))

	// Config Row 1
	assert.NoError(t, collection.Add(&ConfigRowManifest{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "12"},
		AbsPath:      NewAbsPath("main/config-1", "row-1"),
	}))

	// Config Row 2
	assert.NoError(t, collection.Add(&ConfigRowManifest{
		ConfigRowKey: ConfigRowKey{BranchId: 123, ComponentId: "keboola.foo", ConfigId: "678", Id: "34"},
		AbsPath:      NewAbsPath("main/config-1", "row-2"),
	}))

	collection.ResetChanged()
	return collection
}
