package cliconfig_test

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func TestBindFlagsAndEnvToStruct_Default(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")

	target := Config{}
	assert.NoError(t, fs.Parse([]string{}))
	assert.NoError(t, cliconfig.BindFlagsAndEnvToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))
	assert.Equal(t, Config{
		String: "value1",
		Int:    123,
		Float:  4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage: "value2",
	}, target)
}

func TestBindFlagsAndEnvToStruct_Flags(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")

	assert.NoError(t, fs.Parse([]string{
		"--string", "abc",
		"--int", "1000",
		"--float", "78.90",
		"--nested.foo-123", "def",
		"--nested.bar", "2000",
		"--string-with-usage", "",
	}))

	target := Config{}
	assert.NoError(t, cliconfig.BindFlagsAndEnvToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))
	assert.Equal(t, Config{
		String: "abc",
		Int:    1000,
		Float:  78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage: "",
	}, target)
}

func TestBindFlagsAndEnvToStruct_Env(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")

	envs.Set("MY_APP_STRING", "abc")
	envs.Set("MY_APP_INT", "1000")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_FOO_123", "def")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")

	target := Config{}
	assert.NoError(t, cliconfig.BindFlagsAndEnvToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))
	assert.Equal(t, Config{
		String: "abc",
		Int:    1000,
		Float:  78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage: "",
	}, target)
}

func TestBindFlagsAndEnvToViper(t *testing.T) {
	t.Parallel()

	config := Config{String: "default value"}
	flags := []string{
		"--int", "1000",
		"--float", "78.90",
		"--nested.foo-123", "abc",
	}
	envNaming := env.NewNamingConvention("MY_APP_")
	envs := env.Empty()
	envs.Set("MY_APP_NESTED_FOO_123", "def") // not applied, flag has higher priority
	envs.Set("MY_APP_NESTED_BAR", "9999")

	// Generate and parse flags
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	assert.NoError(t, cliconfig.GenerateFlags(config, fs))
	assert.NoError(t, fs.Parse(flags))

	// Bind flags and environment variables to the config struct
	v := viper.New()
	setBy, err := cliconfig.BindFlagsAndEnvToViper(v, fs, envs, envNaming)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, map[string]any{
		"int":               1000,
		"float":             "78.9",
		"string":            "default value",
		"string-with-usage": "",
		"nested": map[string]any{
			"bar":     "9999",
			"foo-123": "abc",
		},
	}, v.AllSettings())
	assert.Equal(t, map[string]cliconfig.SetBy{
		"int":               cliconfig.SetByFlag,
		"float":             cliconfig.SetByFlag,
		"string":            cliconfig.SetByFlagDefault,
		"string-with-usage": cliconfig.SetByFlagDefault,
		"nested.bar":        cliconfig.SetByEnv,
		"nested.foo-123":    cliconfig.SetByFlag,
	}, setBy)
}
