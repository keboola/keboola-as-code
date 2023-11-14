package configmap_test

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ExampleConfig struct {
	Username    string                `configKey:"username" configUsage:"Database username."`
	Password    string                `configKey:"password" configUsage:"Database password." sensitive:"true"`
	Tags        []string              `configKey:"tags" configUsage:"Optional tags."`
	Interactive configmap.Value[bool] `configKey:"interactive"`
}

func (c ExampleConfig) Normalize() {
	c.Username = strings.TrimSpace(c.Username)
}

func (c ExampleConfig) Validate() error {
	if c.Username == "" {
		return errors.New("username is required")
	}
	return nil
}

func ExampleBind_help() {
	// Get ENVs
	envs := env.Empty()

	// Define ENV naming
	envNaming := env.NewNamingConvention("MY_APP_")

	// Define default values
	config := ExampleConfig{
		Username: "default",
	}

	// Create bind specification
	spec := configmap.BindSpec{
		Args:                   []string{"app", "--help"},
		Envs:                   envs,
		EnvNaming:              envNaming,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	// Bind
	helpErr := configmap.Bind(spec, &config)

	// Print help
	help := helpErr.(configmap.HelpError).Help
	help = regexp.MustCompile(` +\n`).ReplaceAllString(help, "\n")
	fmt.Println(help)

	// output:
	//  Usage of "app":
	//       --config-file strings   Path to the configuration file.
	//       --dump-config string    Dump the effective configuration to STDOUT, "json" or "yaml".
	//       --help                  Print help message.
	//       --interactive
	//       --password string       Database password.
	//       --tags strings          Optional tags.
	//       --username string       Database username. (default "default")
	//
	// Configuration source priority: 1. flag, 2. ENV, 3. config file
	//
	// Flags can also be defined as ENV variables.
	// For example, the flag "--foo-bar" becomes the "MY_APP_FOO_BAR" ENV.
	//
	// Use "--config-file" flag to specify a JSON/YAML configuration file, it can be used multiple times.
	//
	// Use "--dump-config" flag with "json" or "yaml" value to dump configuration to STDOUT.
}

func ExampleBind_dump() {
	// Get ENVs
	envs := env.Empty()
	envs.Set("MY_APP_TAGS", "tag1,tag2")

	// Define ENV naming
	envNaming := env.NewNamingConvention("MY_APP_")

	// Define default values
	config := ExampleConfig{
		Username: "default",
	}

	// Create bind specification
	spec := configmap.BindSpec{
		Args:                   []string{"app", "--dump-config=yaml"},
		Envs:                   envs,
		EnvNaming:              envNaming,
		GenerateHelpFlag:       true,
		GenerateConfigFileFlag: true,
		GenerateDumpConfigFlag: true,
	}

	// Bind
	dumpErr := configmap.Bind(spec, &config)

	// Print help
	dump := string(dumpErr.(configmap.DumpError).Dump)
	fmt.Println(dump)

	// output:
	// # Database username.
	// username: default
	// # Database password.
	// password: '*****'
	// # Optional tags.
	// tags:
	//     - tag1
	//     - tag2
	// interactive: false
}
