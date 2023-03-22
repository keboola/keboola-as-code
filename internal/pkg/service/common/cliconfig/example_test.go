package cliconfig_test

import (
	"fmt"
	"regexp"

	"github.com/davecgh/go-spew/spew"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
)

func ExampleGenerateFlags() {
	defaultConfig := Config{String: "default value"}

	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	err := cliconfig.GenerateFlags(defaultConfig, fs)
	if err != nil {
		panic(err)
	}

	// List flags, trim whitespaces
	usage := fs.FlagUsages()
	usage = regexp.MustCompile(`\s+\n`).ReplaceAllString(usage, "\n")
	fmt.Print(usage)

	// output:
	//       --float float
	//       --int int
	//       --nested.bar int
	//       --nested.foo-123 string
	//       --string string               (default "default value")
	//       --string-with-usage string   An usage text.
}

func ExampleBindFlagsAndEnvToStruct() {
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
	err := cliconfig.GenerateFlags(config, fs)
	if err != nil {
		panic(err)
	}
	err = fs.Parse(flags)
	if err != nil {
		panic(err)
	}

	// Bind flags and environment variables to the config struct
	err = cliconfig.BindFlagsAndEnvToStruct(&config, fs, envs, envNaming)
	if err != nil {
		panic(err)
	}

	spew.Dump(config)

	// output:
	// (cliconfig_test.Config) {
	//  Ignored: (string) "",
	//  String: (string) (len=13) "default value",
	//  Int: (int) 1000,
	//  Float: (float64) 78.9,
	//  StringWithUsage: (string) "",
	//  Nested: (cliconfig_test.Nested) {
	//   Ignored: (string) "",
	//   Foo: (string) (len=3) "abc",
	//   Bar: (int) 9999
	//  }
	// }
}

func ExampleBindFlagsAndEnvToViper() {
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
	err := cliconfig.GenerateFlags(config, fs)
	if err != nil {
		panic(err)
	}
	err = fs.Parse(flags)
	if err != nil {
		panic(err)
	}

	// Bind flags and environment variables to the config struct
	v := viper.New()
	_, err = cliconfig.BindFlagsAndEnvToViper(v, fs, envs, envNaming)
	if err != nil {
		panic(err)
	}

	fmt.Print(v.AllSettings())

	// output: map[float:78.9 int:1000 nested:map[bar:9999 foo-123:abc] string:default value string-with-usage:]
}
