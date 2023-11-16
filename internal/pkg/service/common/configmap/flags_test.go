package configmap

import (
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestStructToFlags_AlmostEmpty(t *testing.T) {
	t.Parallel()

	in := TestConfig{Float: 123.45}

	expected := `
      --address string             
      --address-nullable string    
      --custom-int int             
      --custom-string string       
      --duration string            
      --duration-nullable string   
      --embedded string            
      --float float                 (default 123.45)
      --int int                    
      --int-slice ints             
      --nested-bar int             
      --nested-foo string          
      --sensitive-string string    
      --string-slice strings       
      --string-with-usage string   An usage text.
      --url string                 
`

	// Struct
	flagToField1 := make(map[string]orderedmap.Path)
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := StructToFlags(fs1, in, flagToField1)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	flagToField2 := make(map[string]orderedmap.Path)
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = StructToFlags(fs2, &in, flagToField2)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())

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

func TestStructToFlags_Default(t *testing.T) {
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

	expected := `
      --address string              (default "1.2.3.4")
      --address-nullable string     (default "1.2.3.4")
      --custom-int int              (default 567)
      --custom-string string        (default "custom")
      --duration string             (default "2m3s")
      --duration-nullable string    (default "2m3s")
      --embedded string             (default "embedded Value")
      --float float                 (default 4.56)
      --int int                     (default 123)
      --int-slice ints              (default [10,20])
      --nested-bar int              (default 789)
      --nested-foo string           (default "foo")
      --sensitive-string string     (default "value1")
      --string-slice strings        (default [foo,bar])
      --string-with-usage string   An usage text. (default "value2")
      --url string                  (default "http://localhost:1234")
`

	// Struct
	flagToField1 := make(map[string]orderedmap.Path)
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := StructToFlags(fs1, in, flagToField1)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	flagToField2 := make(map[string]orderedmap.Path)
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = StructToFlags(fs2, &in, flagToField2)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())

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
