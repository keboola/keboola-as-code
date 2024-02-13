package cliconfig_test

import (
	"net/netip"
	"net/url"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func TestBindToStruct_Empty(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("embedded", "", "")
	fs.String("string", "", "")
	fs.Int("int", 0, "")
	fs.Float64("float", 0, "")
	fs.String("nested.foo-123", "", "")
	fs.Int("nested.bar", 0, "")
	fs.String("string-with-usage", "", "An usage text.")
	fs.String("duration", "", "")
	fs.String("duration-nullable", "", "")
	fs.String("url", "", "")
	fs.String("address", "", "")
	fs.String("address-nullable", "", "")

	target := Config{}
	assert.NoError(t, fs.Parse([]string{}))
	assert.NoError(t, cliconfig.BindToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))
	assert.Equal(t, Config{}, target)
}

func TestBindToStruct_Default(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("embedded", "default value", "")
	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")
	fs.String("duration", "123s", "")
	fs.String("duration-nullable", "123s", "")
	fs.String("url", "http://localhost:1234", "")
	fs.String("address", "1.2.3.4", "")
	fs.String("address-nullable", "1.2.3.4", "")

	target := Config{}
	assert.NoError(t, fs.Parse([]string{}))
	assert.NoError(t, cliconfig.BindToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))

	duration, _ := time.ParseDuration("123s")
	addrValue := netip.AddrFrom4([4]byte{1, 2, 3, 4})
	assert.Equal(t, Config{
		Embedded: Embedded{EmbeddedField: "default value"},
		String:   "value1",
		Int:      123,
		Float:    4.56,
		Nested: Nested{
			Foo: "foo",
			Bar: 789,
		},
		StringWithUsage:  "value2",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		AddrNullable:     &addrValue,
	}, target)
}

func TestBindToStruct_Flags(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("embedded", "default value", "")
	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")
	fs.String("duration", "123s", "")
	fs.String("duration-nullable", "123s", "")
	fs.String("url", "http://localhost:1234", "")
	fs.String("address", "1.2.3.4", "")
	fs.String("address-nullable", "1.2.3.4", "")

	assert.NoError(t, fs.Parse([]string{
		"--embedded", "foo",
		"--string", "abc",
		"--int", "1000",
		"--float", "78.90",
		"--nested.foo-123", "def",
		"--nested.bar", "2000",
		"--string-with-usage", "",
		"--duration", "100s",
		"--duration-nullable", "100s",
		"--url", "https://foo.bar",
		"--address", "10.20.30.40",
		"--address-nullable", "10.20.30.40",
	}))

	target := Config{}
	assert.NoError(t, cliconfig.BindToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))

	duration, _ := time.ParseDuration("100s")
	addrValue := netip.AddrFrom4([4]byte{10, 20, 30, 40})
	assert.Equal(t, Config{
		Embedded: Embedded{EmbeddedField: "foo"},
		String:   "abc",
		Int:      1000,
		Float:    78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage:  "",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "https", Host: "foo.bar"},
		Addr:             addrValue,
		AddrNullable:     &addrValue,
	}, target)
}

func TestBindToStruct_Env(t *testing.T) {
	t.Parallel()

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	envs := env.Empty()

	fs.String("embedded", "default value", "")
	fs.String("string", "value1", "")
	fs.Int("int", 123, "")
	fs.Float64("float", 4.56, "")
	fs.String("nested.foo-123", "foo", "")
	fs.Int("nested.bar", 789, "")
	fs.String("string-with-usage", "value2", "An usage text.")
	fs.String("duration", "123s", "")
	fs.String("duration-nullable", "123s", "")
	fs.String("url", "http://localhost:1234", "")
	fs.String("address", "1.2.3.4", "")
	fs.String("address-nullable", "1.2.3.4", "")

	envs.Set("MY_APP_EMBEDDED", "foo")
	envs.Set("MY_APP_STRING", "abc")
	envs.Set("MY_APP_INT", "1000")
	envs.Set("MY_APP_FLOAT", "78.90")
	envs.Set("MY_APP_NESTED_FOO_123", "def")
	envs.Set("MY_APP_NESTED_BAR", "2000")
	envs.Set("MY_APP_STRING_WITH_USAGE", "")
	envs.Set("MY_APP_DURATION", "100s")
	envs.Set("MY_APP_DURATION_NULLABLE", "100s")
	envs.Set("MY_APP_ADDRESS", "10.20.30.40")
	envs.Set("MY_APP_ADDRESS_NULLABLE", "10.20.30.40")

	target := Config{}
	assert.NoError(t, cliconfig.BindToStruct(&target, fs, envs, env.NewNamingConvention("MY_APP_")))

	duration, _ := time.ParseDuration("100s")
	addrValue := netip.AddrFrom4([4]byte{10, 20, 30, 40})
	assert.Equal(t, Config{
		Embedded: Embedded{EmbeddedField: "foo"},
		String:   "abc",
		Int:      1000,
		Float:    78.90,
		Nested: Nested{
			Foo: "def",
			Bar: 2000,
		},
		StringWithUsage:  "",
		Duration:         duration,
		DurationNullable: &duration,
		URL:              &url.URL{Scheme: "http", Host: "localhost:1234"},
		Addr:             addrValue,
		AddrNullable:     &addrValue,
	}, target)
}

func TestBindToViper(t *testing.T) {
	t.Parallel()

	config := Config{String: "default value"}
	flags := []string{
		"--embedded", "foo",
		"--int", "1000",
		"--float", "78.90",
		"--nested.foo-123", "abc",
		"--duration", "123s",
		"--duration-nullable", "123s",
		"--url", "https://foo.bar",
		"--address-nullable", "10.20.30.40",
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
	setBy, err := cliconfig.BindToViper(v, fs, envs, envNaming)
	assert.NoError(t, err)

	// Assert
	assert.Equal(t, map[string]any{
		"embedded":          "foo",
		"int":               1000,
		"float":             "78.9",
		"string":            "default value",
		"string-with-usage": "",
		"duration":          "123s",
		"duration-nullable": "123s",
		"url":               "https://foo.bar",
		"address":           "",
		"address-nullable":  "10.20.30.40",
		"nested": map[string]any{
			"bar":     "9999",
			"foo-123": "abc",
		},
	}, v.AllSettings())
	assert.Equal(t, map[string]cliconfig.SetBy{
		"embedded":          cliconfig.SetByFlag,
		"int":               cliconfig.SetByFlag,
		"float":             cliconfig.SetByFlag,
		"string":            cliconfig.SetByFlagDefault,
		"string-with-usage": cliconfig.SetByFlagDefault,
		"duration":          cliconfig.SetByFlag,
		"duration-nullable": cliconfig.SetByFlag,
		"url":               cliconfig.SetByFlag,
		"address":           cliconfig.SetByFlagDefault,
		"address-nullable":  cliconfig.SetByFlag,
		"nested.bar":        cliconfig.SetByEnv,
		"nested.foo-123":    cliconfig.SetByFlag,
	}, setBy)
}
