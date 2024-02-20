package configpatch_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
)

type Config struct {
	Key1 []string      `json:"foo1"`
	Key2 int           `json:"foo2" alternative:"baz2" protected:"true"`
	Key3 ConfigNested1 `json:"foo3,omitempty" alternative:"baz3"`
}

type ConfigNested1 struct {
	Key4 string        `json:"foo4"`
	Key5 int           `json:"foo5" alternative:"baz5"`
	Key6 ConfigNested2 `json:"foo6,omitempty"`
}

type ConfigNested2 struct {
	Key7 []string `json:"foo7"`
	Key8 bool     `json:"foo8" protected:"true"`
}

type ConfigPatch struct {
	Key1 *[]string           `json:"foo1"`
	Key2 *int                `json:"foo2" alternative:"baz2"`
	Key3 *ConfigNested1Patch `json:"foo3,omitempty" alternative:"baz3"`
}

type ConfigNested1Patch struct {
	Key5 *int                `json:"foo5" alternative:"baz5"`
	Key6 *ConfigNested2Patch `json:"foo6,omitempty"`
}

type ConfigNested2Patch struct {
	Key7 *[]string `json:"foo7"`
	Key8 *bool     `json:"foo8"`
}

type ConfigPatchInvalid struct {
	Key1 string  `json:"foo1"`
	Key2 *string `json:"foo2"`
	Key8 *string `json:"foo8"`
	Key9 *int    `json:"foo9"`
}

func TestDumpAll_EmptyPatch(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		newConfig(),
		ConfigPatch{},
	)

	require.NoError(t, err)
	assert.Equal(t, []configpatch.ConfigKV{
		{
			KeyPath:      "foo1",
			Value:        []string{"bar1"},
			DefaultValue: []string{"bar1"},
		},
		{
			KeyPath:      "foo2",
			Value:        123,
			DefaultValue: 123,
			Protected:    true,
		},
		{
			KeyPath:      "foo3.foo5",
			Value:        234,
			DefaultValue: 234,
		},
		{
			KeyPath:      "foo3.foo6.foo7",
			Value:        []string{"bar7"},
			DefaultValue: []string{"bar7"},
		},
		{
			KeyPath:      "foo3.foo6.foo8",
			Value:        true,
			DefaultValue: true,
			Protected:    true,
		},
	}, kvs)
}

func TestDumpAll_Ok(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		newConfig(),
		newConfigPatch(),
	)

	require.NoError(t, err)
	assert.Equal(t, []configpatch.ConfigKV{
		{
			KeyPath:      "foo1",
			Value:        []string{"patch1"},
			DefaultValue: []string{"bar1"},
			Overwritten:  true,
		},
		{
			KeyPath:      "foo2",
			Value:        123,
			DefaultValue: 123,
			Protected:    true,
		},
		{
			KeyPath:      "foo3.foo5",
			Value:        789,
			DefaultValue: 234,
			Overwritten:  true,
		},
		{
			KeyPath:      "foo3.foo6.foo7",
			Value:        []string{"patch7"},
			DefaultValue: []string{"bar7"},
			Overwritten:  true,
		},
		{
			KeyPath:      "foo3.foo6.foo8",
			Value:        true,
			DefaultValue: true,
			Protected:    true,
		},
	}, kvs)
}

func TestDumpAll_EmptyPatchPointer(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		newConfig(),
		(*ConfigPatch)(nil),
	)

	require.NoError(t, err)
	assert.Equal(t, []configpatch.ConfigKV{
		{
			KeyPath:      "foo1",
			Value:        []string{"bar1"},
			DefaultValue: []string{"bar1"},
		},
		{
			KeyPath:      "foo2",
			Value:        123,
			DefaultValue: 123,
			Protected:    true,
		},
		{
			KeyPath:      "foo3.foo5",
			Value:        234,
			DefaultValue: 234,
		},
		{
			KeyPath:      "foo3.foo6.foo7",
			Value:        []string{"bar7"},
			DefaultValue: []string{"bar7"},
		},
		{
			KeyPath:      "foo3.foo6.foo8",
			Value:        true,
			DefaultValue: true,
			Protected:    true,
		},
	}, kvs)
}

func TestDumpAll_CustomTags(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		newConfig(),
		ConfigPatch{
			Key2: ptr(234),
			Key3: &ConfigNested1Patch{
				Key5: ptr(345),
			},
		},
		configpatch.WithNameTag("alternative"),   // <<<<<
		configpatch.WithProtectedTag("notfound"), // <<<<<
	)

	require.NoError(t, err)
	assert.Equal(t, []configpatch.ConfigKV{
		{
			KeyPath:      "baz2",
			Value:        234,
			DefaultValue: 123,
			Overwritten:  true,
		},
		{
			KeyPath:      "baz3.baz5",
			Value:        345,
			DefaultValue: 234,
			Overwritten:  true,
		},
	}, kvs)
}

func TestDumpAll_InvalidPatch(t *testing.T) {
	t.Parallel()

	_, err := configpatch.DumpAll(
		newConfig(),
		newConfigPatchInvalid(),
	)

	if assert.Error(t, err) {
		assert.Equal(t, strings.TrimSpace(`
- patch field "foo1" is not a pointer, but "string"
- patch field "foo2" type "string" doesn't match config field type "int"
- patch contains unexpected keys: "foo8", "foo9"
`), err.Error())
	}
}

func TestDumpAll_Protected_Ok(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpAll(
		newConfig(),
		newConfigPatchProtected(),
		configpatch.WithModifyProtected(), // <<<<<
	)

	require.NoError(t, err)
	assert.Equal(t, []configpatch.ConfigKV{
		{
			KeyPath:      "foo1",
			Value:        []string{"bar1"},
			DefaultValue: []string{"bar1"},
		},
		{
			KeyPath:      "foo2",
			Value:        567,
			DefaultValue: 123,
			Overwritten:  true,
			Protected:    true,
		},
		{
			KeyPath:      "foo3.foo5",
			Value:        789,
			DefaultValue: 234,
			Overwritten:  true,
		},
		{
			KeyPath:      "foo3.foo6.foo7",
			Value:        []string{"bar7"},
			DefaultValue: []string{"bar7"},
			Overwritten:  false,
		},
		{
			KeyPath:      "foo3.foo6.foo8",
			Value:        true,
			DefaultValue: true,
			Overwritten:  true,
			Protected:    true,
		},
	}, kvs)
}

func TestDumpAll_Protected_Error(t *testing.T) {
	t.Parallel()
	_, err := configpatch.DumpAll(newConfig(), newConfigPatchProtected())
	if assert.Error(t, err) {
		assert.Equal(t, `cannot modify protected fields: "foo2", "foo3.foo6.foo8"`, err.Error())
	}
}

func TestDumpPatch_EmptyPatch(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpPatch(
		newConfig(),
		ConfigPatch{},
	)

	require.NoError(t, err)
	assert.Empty(t, kvs)
}

func TestDumpPatch_EmptyPatchPointer(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpPatch(
		newConfig(),
		&ConfigPatch{},
	)

	require.NoError(t, err)
	assert.Empty(t, kvs)
}

func TestDumpPatch_Ok(t *testing.T) {
	t.Parallel()

	kvs, err := configpatch.DumpPatch(
		newConfig(),
		newConfigPatch(),
	)

	require.NoError(t, err)
	assert.Equal(t, configpatch.PatchKVs{
		{
			KeyPath: "foo1",
			Value:   []string{"patch1"},
		},
		{
			KeyPath: "foo3.foo5",
			Value:   789,
		},
		{
			KeyPath: "foo3.foo6.foo7",
			Value:   []string{"patch7"},
		},
	}, kvs)
}

func newConfig() Config {
	return Config{
		Key1: []string{"bar1"},
		Key2: 123,
		Key3: ConfigNested1{
			Key4: "bar4",
			Key5: 234,
			Key6: ConfigNested2{
				Key7: []string{"bar7"},
				Key8: true,
			},
		},
	}
}

func newConfigPatch() ConfigPatch {
	return ConfigPatch{
		Key1: ptr([]string{"patch1"}),
		Key3: &ConfigNested1Patch{
			Key5: ptr(789),
			Key6: &ConfigNested2Patch{
				Key7: ptr([]string{"patch7"}),
			},
		},
	}
}

func newConfigPatchProtected() ConfigPatch {
	return ConfigPatch{
		Key2: ptr(567),
		Key3: &ConfigNested1Patch{
			Key5: ptr(789),
			Key6: &ConfigNested2Patch{
				Key8: ptr(true),
			},
		},
	}
}

func newConfigPatchInvalid() ConfigPatchInvalid {
	return ConfigPatchInvalid{
		Key1: "patch1",
		Key2: ptr("patch2"),
		Key8: ptr("patch8"),
		Key9: ptr(789),
	}
}

func ptr[T any](v T) *T {
	return &v
}
