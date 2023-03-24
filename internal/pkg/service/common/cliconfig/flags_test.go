package cliconfig_test

import (
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func TestGenerateFlags_NotStruct(t *testing.T) {
	t.Parallel()

	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := cliconfig.GenerateFlags("invalid", fs1)
	if assert.Error(t, err) {
		assert.Equal(t, `type "string" is not a struct or a pointer to a struct`, err.Error())
	}
}

func TestGenerateFlags_Empty(t *testing.T) {
	t.Parallel()

	in := Config{}

	expected := `
      --address string             
      --address-nullable string    
      --duration string            
      --duration-nullable string   
      --embedded string            
      --float float                
      --int int                    
      --nested.bar int             
      --nested.foo-123 string      
      --string string              
      --string-with-usage string   An usage text.
      --url string                 
`

	// Struct
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := cliconfig.GenerateFlags(in, fs1)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = cliconfig.GenerateFlags(&in, fs2)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())
}

func TestGenerateFlags_Default(t *testing.T) {
	t.Parallel()

	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	in := Config{
		Embedded:         Embedded{EmbeddedField: "embedded value"},
		String:           "value1",
		Int:              123,
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
      --duration string             (default "2m3s")
      --duration-nullable string    (default "2m3s")
      --embedded string             (default "embedded value")
      --float float                 (default 4.56)
      --int int                     (default 123)
      --nested.bar int              (default 789)
      --nested.foo-123 string       (default "foo")
      --string string               (default "value1")
      --string-with-usage string   An usage text. (default "value2")
      --url string                  (default "http://localhost:1234")
`

	// Struct
	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := cliconfig.GenerateFlags(in, fs1)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs1.FlagUsages())

	// Struct pointer
	fs2 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err = cliconfig.GenerateFlags(&in, fs2)
	assert.NoError(t, err)
	assert.Equal(t, strings.TrimLeft(expected, "\n"), fs2.FlagUsages())
}
