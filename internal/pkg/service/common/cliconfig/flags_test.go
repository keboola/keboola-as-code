package cliconfig_test

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func TestGenerateFlags_NotStruct(t *testing.T) {
	t.Parallel()

	fs1 := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := cliconfig.GenerateFlags("invalid", fs1)
	if assert.Error(t, err) {
		assert.Equal(t, `type "string" is not a struct or a pointer to a struct, it cannot be mapped to the FlagSet`, err.Error())
	}
}

func TestGenerateFlags_Empty(t *testing.T) {
	t.Parallel()

	in := Config{}

	expected := `
      --float float                
      --int int                    
      --nested.bar int             
      --nested.foo-123 string      
      --string string              
      --string-with-usage string   An usage text.
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

	in := Config{
		String:          "value1",
		Int:             123,
		Float:           4.56,
		StringWithUsage: "value2",
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
	}

	expected := `
      --float float                 (default 4.56)
      --int int                     (default 123)
      --nested.bar int              (default 789)
      --nested.foo-123 string       (default "foo")
      --string string               (default "value1")
      --string-with-usage string   An usage text. (default "value2")
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
