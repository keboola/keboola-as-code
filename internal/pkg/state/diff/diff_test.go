package diff_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local/naming"
	. "github.com/keboola/keboola-as-code/internal/pkg/state/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/state/sort"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/orderedmap"
)

func TestDiff_WithSorter(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Object are NOT sorted by path
	key1 := model.BranchKey{BranchId: 1}
	key2 := model.BranchKey{BranchId: 2}
	key3 := model.BranchKey{BranchId: 3}
	key4 := model.BranchKey{BranchId: 4}
	A.MustAdd(&model.Branch{BranchKey: key1})
	A.MustAdd(&model.Branch{BranchKey: key2})
	A.MustAdd(&model.Branch{BranchKey: key3})
	A.MustAdd(&model.Branch{BranchKey: key4})
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(key1, model.NewAbsPath("", "003"))
	namingReg.MustAttach(key2, model.NewAbsPath("", "001"))
	namingReg.MustAttach(key3, model.NewAbsPath("", "002"))
	namingReg.MustAttach(key4, model.NewAbsPath("", "004"))

	// Do diff, use path sorter
	results, err := Diff(A, B, WithSorter(sort.NewPathSorter(namingReg)))
	assert.NoError(t, err)
	assert.Len(t, results.Results, 4)

	// Diff results are sorted by path
	assert.Equal(t, strings.TrimLeft(`
- B 001
- B 002
- B 003
- B 004
`, "\n"), results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

// Equal ---------------------------------------------------------------------------------------------------------------

func TestDiff_Equal_Branch(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Branch is equal in the A and B
	branchKey := model.BranchKey{BranchId: 123}
	aBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	A.MustAdd(aBranch)
	bBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	B.MustAdd(bBranch)

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	// Result state is ResultEqual
	result := results.Results[0]
	assert.Equal(t, ResultEqual, result.State)

	// Changed fields are empty
	//assert.True(t, result.ChangedFields.IsEmpty())

	// Result A and B objects are set
	assert.Same(t, aBranch, result.A.Object().(*model.Branch))
	assert.Same(t, bBranch, result.B.Object().(*model.Branch))

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branchKey, model.NewAbsPath("", "my-branch"))

	// No result by default
	assert.Equal(t, "", results.Format(WithDetails()))

	// Formatted result without details
	assert.Equal(t, "", results.Format())

	// Formatted result with details
	assert.Equal(t, "", results.Format(WithDetails()))

	// Formatted result without details + include equal results
	assert.Equal(t, "= B branch:123\n", results.Format(WithEqualResults()))

	// Formatted result with details + include equal results
	assert.Equal(t, "= B branch:123\n", results.Format(WithEqualResults()), WithDetails())

	// Formatted result without details + path is known  + include equal results
	assert.Equal(t, "= B my-branch\n", results.Format(WithEqualResults(), WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known  + include equal results
	assert.Equal(t, "= B my-branch\n", results.Format(WithEqualResults(), WithNamingRegistry(namingReg), WithDetails()))
}

func TestDiff_EqualConfig(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}
	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "A", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "config name",
		Description:       "description",
		ChangeDescription: "B", // no diff:"true" tag
	})

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	// Config result state is ResultEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultEqual, result.State)
	assert.True(t, found)

	// Changed fields are empty
	//assert.True(t, result.ChangedFields.IsEmpty())

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branchKey, model.NewAbsPath("", "my-branch"))
	namingReg.MustAttach(configKey, model.NewAbsPath("my-branch", "my-config"))

	// No result by default
	assert.Equal(t, "", results.Format(WithDetails()))

	// Formatted result without details
	assert.Equal(t, "", results.Format())

	// Formatted result with details
	assert.Equal(t, "", results.Format(WithDetails()))

	// Formatted result without details + include equal results
	assert.Equal(t, strings.TrimLeft(`
= B branch:123
= C branch:123/component:foo-bar/config:456
`, "\n"), results.Format(WithEqualResults()))

	// Formatted result with details + include equal results
	assert.Equal(t, strings.TrimLeft(`
= B branch:123
= C branch:123/component:foo-bar/config:456
`, "\n"), results.Format(WithEqualResults()), WithDetails())

	// Formatted result without details + path is known  + include equal results
	assert.Equal(t, strings.TrimLeft(`
= B my-branch
= C my-branch/my-config
`, "\n"), results.Format(WithEqualResults(), WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known  + include equal results
	assert.Equal(t, strings.TrimLeft(`
= B my-branch
= C my-branch/my-config
`, "\n"), results.Format(WithEqualResults(), WithNamingRegistry(namingReg), WithDetails()))
}

// Only in A/B ---------------------------------------------------------------------------------------------------------

func TestDiff_OnlyInA(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Branch exists only in A
	branch := &model.Branch{BranchKey: model.BranchKey{BranchId: 123}}
	A.MustAdd(branch)

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	// Result state is ResultOnlyInA
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInA, result.State)

	// Object in B does not exist at all, so changed fields are empty
	//assert.True(t, result.ChangedFields.IsEmpty())

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branch.Key(), model.NewAbsPath("", "my-branch"))

	// Result A object is set, B object is nil
	assert.Same(t, branch, result.A.Object())
	assert.Nil(t, result.B.Object())

	// Formatted result without details
	assert.Equal(t, "- B branch:123\n", results.Format())

	// Formatted result with details
	assert.Equal(t, "- B branch:123\n", results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "- B my-branch\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, "- B my-branch\n", results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

func TestDiff_OnlyInB(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Branch exists only in B
	branch := &model.Branch{BranchKey: model.BranchKey{BranchId: 123}}
	B.MustAdd(branch)

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	// Result state is ResultOnlyInB
	result := results.Results[0]
	assert.Equal(t, ResultOnlyInB, result.State)

	// Object in A does not exist at all, so changed fields are empty
	//assert.True(t, result.ChangedFields.IsEmpty())

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branch.Key(), model.NewAbsPath("", "my-branch"))

	// Result B object is defined, A object is nil
	assert.Nil(t, result.A.Object())
	assert.Same(t, branch, result.B.Object())

	// Formatted result without details
	assert.Equal(t, "+ B branch:123\n", results.Format())

	// Formatted result with details
	assert.Equal(t, "+ B branch:123\n", results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "+ B my-branch\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, "+ B my-branch\n", results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

// Not Equal -----------------------------------------------------------------------------------------------------------

func TestDiff_NotEqual_Branch(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Branch Name and IsDefault are different in A and B
	branchKey := model.BranchKey{BranchId: 123}
	aBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "name",
		Description: "description",
		IsDefault:   false,
	}
	A.MustAdd(aBranch)
	bBranch := &model.Branch{
		BranchKey:   branchKey,
		Name:        "changed",
		Description: "description",
		IsDefault:   true,
	}
	B.MustAdd(bBranch)

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 1)

	// Result state is ResultNotEqual
	result := results.Results[0]
	assert.Equal(t, ResultNotEqual, result.State)

	// Changed fields
	//assert.Equal(t, `isDefault, name`, result.ChangedFields.String())
	//assert.Equal(t, "  - name\n  + changed", result.ChangedFields.Get("name").Diff())
	//assert.Equal(t, "  - false\n  + true", result.ChangedFields.Get("isDefault").Diff())

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(branchKey, model.NewAbsPath("", "my-branch"))

	// Result A and B objects are set
	assert.Same(t, aBranch, result.A.Object().(*model.Branch))
	assert.Same(t, bBranch, result.B.Object().(*model.Branch))

	// Formatted result without details
	assert.Equal(t, "* B branch:123 | changes: isDefault, name\n", results.Format())

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
* B branch:123
    name
    - name
    + changed
    isDefault
    - false
    + true
`, "\n"), results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "* B my-branch | changes: isDefault, name\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
* B my-branch
    name
    - name
    + changed
    isDefault
    - false
    + true
`, "\n"), results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

func TestDiff_NotEqual_Config(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Config Name and Description are different in A and B
	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}
	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "A", // no diff:"true" tag
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "changed",
		Description:       "changed",
		ChangeDescription: "B", // no diff:"true" tag
	})

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	// Config result state is ResultNotEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultNotEqual, result.State)
	assert.True(t, found)
	//assert.Equal(t, `description, name`, result2.ChangedFields.String())

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(configKey, model.NewAbsPath("my-branch", "my-config"))

	// Formatted result without details
	assert.Equal(t, "* C branch:123/component:foo-bar/config:456 | changes: description, name\n", results.Format())

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
* C branch:123/component:foo-bar/config:456
    name
    - name
    + changed
    description
    - description
    + changed
`, "\n"), results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "* C my-branch/my-config | changes: description, name\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
* C my-branch/my-config
    name
    - name
    + changed
    description
    - description
    + changed
`, "\n"), results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

func TestDiff_NotEqual_Config_Configuration(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Config Configuration are different in A and B
	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}
	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "remote", // no diff:"true" tag
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "456"},
				}),
			},
		}),
	})

	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey:         configKey,
		Name:              "name",
		Description:       "description",
		ChangeDescription: "local", // no diff:"true" tag
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "123"},
				}),
			},
		}),
	})

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	// Config result state is ResultNotEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultNotEqual, result.State)
	assert.True(t, found)

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(configKey, model.NewAbsPath("my-branch", "my-config"))

	// Formatted result without details
	assert.Equal(t, "* C branch:123/component:foo-bar/config:456 | changes: configuration\n", results.Format())

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
* C branch:123/component:foo-bar/config:456
    configuration.foo.bar
    - 456
    + 123
`, "\n"), results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "* C my-branch/my-config | changes: configuration\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
* C my-branch/my-config
    configuration.foo.bar
    - 456
    + 123
`, "\n"), results.Format(WithNamingRegistry(namingReg), WithDetails()))
}

func TestDiff_NotEqual_Config_Configuration_Map(t *testing.T) {
	t.Parallel()
	A, B := state.NewCollection(), state.NewCollection()

	// Config Configuration are different in A and B
	branchKey := model.BranchKey{BranchId: 123}
	configKey := model.ConfigKey{BranchKey: branchKey, ComponentId: "foo-bar", ConfigId: "456"}
	A.MustAdd(&model.Branch{BranchKey: branchKey})
	A.MustAdd(&model.Config{
		ConfigKey: configKey,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{
						Key: "bar",
						Value: orderedmap.FromPairs([]orderedmap.Pair{
							{
								Key: "baz",
								Value: orderedmap.FromPairs([]orderedmap.Pair{
									{Key: "key", Value: "value"},
								}),
							},
						}),
					},
				}),
			},
		}),
	})
	B.MustAdd(&model.Branch{BranchKey: branchKey})
	B.MustAdd(&model.Config{
		ConfigKey: configKey,
		Content: orderedmap.FromPairs([]orderedmap.Pair{
			{
				Key: "foo",
				Value: orderedmap.FromPairs([]orderedmap.Pair{
					{Key: "bar", Value: "value"},
				}),
			},
			{Key: "key", Value: "value"},
		}),
	})

	// Do diff
	results, err := Diff(A, B)
	assert.NoError(t, err)
	assert.Len(t, results.Results, 2)

	// Config result state is ResultNotEqual
	result, found := results.Get(configKey)
	assert.Equal(t, ResultNotEqual, result.State)
	assert.True(t, found)

	// Setup naming registry
	namingReg := naming.NewRegistry()
	namingReg.MustAttach(configKey, model.NewAbsPath("my-branch", "my-config"))

	// Formatted result without details
	assert.Equal(t, "* C branch:123/component:foo-bar/config:456 | changes: configuration\n", results.Format())

	// Formatted result with details
	assert.Equal(t, strings.TrimLeft(`
* C branch:123/component:foo-bar/config:456
    configuration.foo.bar
    - {
    -   "baz": {
    -     "key": "value"
    -   }
    - }
    + "value"
  + configuration.key
  +   value
`, "\n"), results.Format(WithDetails()))

	// Formatted result without details + path is known
	assert.Equal(t, "* C my-branch/my-config | changes: configuration\n", results.Format(WithNamingRegistry(namingReg)))

	// Formatted result with details + path is known
	assert.Equal(t, strings.TrimLeft(`
* C my-branch/my-config
    configuration.foo.bar
    - {
    -   "baz": {
    -     "key": "value"
    -   }
    - }
    + "value"
  + configuration.key
  +   value
`, "\n"), results.Format(WithNamingRegistry(namingReg), WithDetails()))
}
