package configmap

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

func TestGenerateAndBind_Help_Minimal(t *testing.T) {
	t.Parallel()

	cfg := GenerateAndBindConfig{
		Args:                   []string{"app", "--help"},
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: false,
		GenerateDumpConfigFlag: false,
	}

	target := TestConfig{}

	expected := `
Usage of "app":
      --address string             
      --address-nullable string    
      --byte-slice string          
      --custom-int int             
      --custom-string string       
      --duration string            
      --duration-nullable string   
      --embedded string            
      --float float                
      --help                       Print help message.
      --int int                    
      --int-slice ints             
      --nested-bar int             
      --nested-foo string          
      --sensitive-string string    
      --string-slice strings       
      --string-with-usage string   An usage text.
  -u, --url string                 
`

	err := GenerateAndBind(cfg, &target)
	if assert.Error(t, err) {
		helpErr, ok := err.(HelpError)
		require.True(t, ok)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(helpErr.Help))
		assert.Equal(t, "help requested", helpErr.Error())
	}
}

func TestGenerateAndBind_Help_Full(t *testing.T) {
	t.Parallel()

	cfg := GenerateAndBindConfig{
		Args:                   []string{"app", "--help"},
		EnvNaming:              env.NewNamingConvention("MY_APP_"),
		Envs:                   env.Empty(),
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	target := TestConfig{}

	expected := `
Usage of "app":
      --address string             
      --address-nullable string    
      --byte-slice string          
      --config-file strings        Path to the configuration file.
      --custom-int int             
      --custom-string string       
      --dump-config string         Dump the effective configuration to STDOUT, "json" or "yaml".
      --duration string            
      --duration-nullable string   
      --embedded string            
      --float float                
      --help                       Print help message.
      --int int                    
      --int-slice ints             
      --nested-bar int             
      --nested-foo string          
      --sensitive-string string    
      --string-slice strings       
      --string-with-usage string   An usage text.
  -u, --url string                 

Configuration source priority: 1. flag, 2. ENV, 3. config file

Flags can also be defined as ENV variables.
For example, the flag "--foo-bar" becomes the "MY_APP_FOO_BAR" ENV.

Use "--config-file" flag to specify a JSON/YAML configuration file, it can be used multiple times.

Use "--dump-config" flag with "json" or "yaml" value to dump configuration to STDOUT.
`

	err := GenerateAndBind(cfg, &target)
	if assert.Error(t, err) {
		helpErr, ok := err.(HelpError)
		require.True(t, ok)
		assert.Equal(t, strings.TrimSpace(expected), strings.TrimSpace(helpErr.Help))
		assert.Equal(t, "help requested", helpErr.Error())
	}
}
