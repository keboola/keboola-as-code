package configmap

import (
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldToFlagName(t *testing.T) {
	t.Parallel()

	cases := []struct{ FieldName, ExpectedFlagName string }{
		{FieldName: "", ExpectedFlagName: ""},
		{FieldName: "  ", ExpectedFlagName: ""},
		{FieldName: "foo", ExpectedFlagName: "foo"},
		{FieldName: "Foo", ExpectedFlagName: "foo"},
		{FieldName: "foo-bar", ExpectedFlagName: "foo-bar"},
		{FieldName: "fooBar", ExpectedFlagName: "foo-bar"},
		{FieldName: "FooBar", ExpectedFlagName: "foo-bar"},
		{FieldName: "---Foo---Bar---", ExpectedFlagName: "foo-bar"},
	}

	for _, tc := range cases {
		assert.Equal(t, tc.ExpectedFlagName, fieldToFlagName(tc.FieldName))
	}
}

func TestFlagToFieldMap_AlmostEmpty(t *testing.T) {
	t.Parallel()

	in := TestConfig{Float: 123.45}

	// Struct
	flagToField1 := make(map[string]orderedmap.Path)
	err := flagFieldMapTo(in, flagToField1)
	require.NoError(t, err)

	// Struct pointer
	flagToField2 := make(map[string]orderedmap.Path)
	err = flagFieldMapTo(&in, flagToField2)
	require.NoError(t, err)

	// Check flag to field mapping
	expectedFlagToField := map[string]orderedmap.Path{
		"address":           orderedmap.PathFromStr("address"),
		"address-nullable":  orderedmap.PathFromStr("addressNullable"),
		"custom-int":        orderedmap.PathFromStr("customInt"),
		"custom-string":     orderedmap.PathFromStr("customString"),
		"duration":          orderedmap.PathFromStr("duration"),
		"duration-nullable": orderedmap.PathFromStr("durationNullable"),
		"embedded":          orderedmap.PathFromStr("embedded"),
		"float":             orderedmap.PathFromStr("float"),
		"int":               orderedmap.PathFromStr("int"),
		"int-slice":         orderedmap.PathFromStr("intSlice"),
		"nested-bar":        orderedmap.PathFromStr("nested.bar"),
		"nested-foo":        orderedmap.PathFromStr("nested.foo"),
		"sensitive-string":  orderedmap.PathFromStr("sensitiveString"),
		"string-slice":      orderedmap.PathFromStr("stringSlice"),
		"string-with-usage": orderedmap.PathFromStr("stringWithUsage"),
		"url":               orderedmap.PathFromStr("url"),
	}
	assert.Equal(t, expectedFlagToField, flagToField1)
	assert.Equal(t, expectedFlagToField, flagToField2)
}

func TestFlagToFieldMap_Default(t *testing.T) {
	t.Parallel()

	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	in := TestConfig{
		Embedded:         Embedded{EmbeddedField: "embedded Value"},
		CustomString:     "custom",
		CustomInt:        567,
		SensitiveString:  "value1",
		StringSlice:      []string{"foo", "bar"},
		Int:              123,
		IntSlice:         []int{10, 20},
		Float:            4.56,
		StringWithUsage:  "value2",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		AddrNullable:     &addrValue,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
	}

	// Struct
	flagToField1 := make(map[string]orderedmap.Path)
	require.NoError(t, flagFieldMapTo(in, flagToField1))

	// Struct pointer
	flagToField2 := make(map[string]orderedmap.Path)
	require.NoError(t, flagFieldMapTo(&in, flagToField2))

	// Check flag to field mapping
	expectedFlagToField := map[string]orderedmap.Path{
		"address":           orderedmap.PathFromStr("address"),
		"address-nullable":  orderedmap.PathFromStr("addressNullable"),
		"custom-int":        orderedmap.PathFromStr("customInt"),
		"custom-string":     orderedmap.PathFromStr("customString"),
		"duration":          orderedmap.PathFromStr("duration"),
		"duration-nullable": orderedmap.PathFromStr("durationNullable"),
		"embedded":          orderedmap.PathFromStr("embedded"),
		"float":             orderedmap.PathFromStr("float"),
		"int":               orderedmap.PathFromStr("int"),
		"int-slice":         orderedmap.PathFromStr("intSlice"),
		"nested-bar":        orderedmap.PathFromStr("nested.bar"),
		"nested-foo":        orderedmap.PathFromStr("nested.foo"),
		"sensitive-string":  orderedmap.PathFromStr("sensitiveString"),
		"string-slice":      orderedmap.PathFromStr("stringSlice"),
		"string-with-usage": orderedmap.PathFromStr("stringWithUsage"),
		"url":               orderedmap.PathFromStr("url"),
	}
	assert.Equal(t, expectedFlagToField, flagToField1)
	assert.Equal(t, expectedFlagToField, flagToField2)
}
