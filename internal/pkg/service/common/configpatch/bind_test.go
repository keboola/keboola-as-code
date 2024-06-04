package configpatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
)

type ConfigPatchTextUnmarshaller struct {
	Duration *time.Duration `json:"duration"`
}

func TestBindKVs_NoStructPointer(t *testing.T) {
	t.Parallel()
	assert.PanicsWithError(t, `patch struct must be a pointer to a struct, found "configpatch_test.ConfigPatch"`, func() {
		var kvs []configpatch.PatchKV
		_ = configpatch.BindKVs(ConfigPatch{}, kvs)
	})
}

func TestBindKVs_Empty(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	var kvs []configpatch.PatchKV
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatch{}, patch)
}

func TestBindKVs_One(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo3.foo5", Value: 789},
	}
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatch{
		Key3: &ConfigNested1Patch{Key5: ptr.Ptr(789)},
	}, patch)
}

func TestBindKVs_DeepNested(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo3.foo6.foo8", Value: false},
	}
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatch{
		Key3: &ConfigNested1Patch{
			Key6: &ConfigNested2Patch{
				Key8: ptr.Ptr(false),
			},
		},
	}, patch)
}

func TestBindKVs_Multiple(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo1", Value: []any{"bar1"}}, // slice []any -> []string
		{KeyPath: "foo3.foo5", Value: 789},
	}
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatch{
		Key1: ptr.Ptr([]string{"bar1"}),
		Key3: &ConfigNested1Patch{Key5: ptr.Ptr(789)},
	}, patch)
}

func TestBindKVs_UnmarshalText_Ok(t *testing.T) {
	t.Parallel()
	patch := ConfigPatchTextUnmarshaller{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "duration", Value: "1h20m"},
	}
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatchTextUnmarshaller{
		Duration: ptr.Ptr(time.Hour + 20*time.Minute),
	}, patch)
}

func TestBindKVs_UnmarshalText_Error(t *testing.T) {
	t.Parallel()
	patch := ConfigPatchTextUnmarshaller{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "duration", Value: "invalid value"},
	}
	if err := configpatch.BindKVs(&patch, kvs); assert.Error(t, err) {
		assert.Equal(t, `invalid "duration": time: invalid duration "invalid value"`, err.Error())
	}
}

func TestBindKVs_CompatibleType(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo3.foo5", Value: float64(789)}, // different, but compatible type
	}
	require.NoError(t, configpatch.BindKVs(&patch, kvs))
	assert.Equal(t, ConfigPatch{
		Key3: &ConfigNested1Patch{Key5: ptr.Ptr(789)},
	}, patch)
}

func TestBindKVs_IncompatibleType(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo3.foo5", Value: "789"},
	} // incompatible type, expected int
	if err := configpatch.BindKVs(&patch, kvs); assert.Error(t, err) {
		assert.Equal(t, `invalid "foo3.foo5" value: found type "string", expected "int"`, err.Error())
	}
}

func TestBindKVs_DuplicateKV(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: "foo3.foo5", Value: 789},
		{KeyPath: "foo3.foo5", Value: 789},
	}
	if err := configpatch.BindKVs(&patch, kvs); assert.Error(t, err) {
		assert.Equal(t, `key "foo3.foo5" is defined multiple times`, err.Error())
	}
}

func TestBindKVs_KeyNotFound(t *testing.T) {
	t.Parallel()
	patch := ConfigPatch{}
	kvs := []configpatch.PatchKV{
		{KeyPath: `foo.bar.1`, Value: 123},
		{KeyPath: `foo.bar.2`, Value: 234},
	}
	if err := configpatch.BindKVs(&patch, kvs); assert.Error(t, err) {
		assert.Equal(t, `key not found: "foo.bar.1", "foo.bar.2"`, err.Error())
	}
}

func TestBindKVs_InvalidPatchStruct(t *testing.T) {
	t.Parallel()
	patch := ConfigPatchInvalid{}
	var kvs []configpatch.PatchKV
	if err := configpatch.BindKVs(&patch, kvs); assert.Error(t, err) {
		assert.Equal(t, `patch field "foo1" is not a pointer, but "string"`, err.Error())
	}
}
