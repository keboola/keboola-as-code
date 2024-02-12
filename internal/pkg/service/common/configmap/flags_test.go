package configmap

import (
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestGenerateFlags_AlmostEmpty(t *testing.T) {
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
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := GenerateFlags(fs1, in)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = GenerateFlags(fs2, &in)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())
}

func TestGenerateFlags_Default(t *testing.T) {
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
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := GenerateFlags(fs1, in)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = GenerateFlags(fs2, &in)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())
}
