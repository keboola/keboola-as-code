// Package configmap provides bidirectional mapping from flags, ENVs, and configuration files to configuration structures.
//
// Start with the [Bind] function.
//
// # Features
//
//   - Automatic generation of flags from a configuration structure, see the [Bind] function.
//   - Ability to set flags also as environment variables.
//   - Support for configuration files.
//   - Generic type Value[T] can be used to get value source, see Value.SetBy.
//   - Dumping of the configuration in JSON or YAML format, see the [NewDumper] function.
//   - Ability to mask sensitive values in dump with the sensitive tag.
//
// # Configuration Structure
//
// At first, create a configuration structure. The following field tags can be used:
//   - configKey - name of the field in the configuration in camelCase style
//   - configUsage - flag usage message
//   - sensitive - if "true", then the field value is masked in the dump output
//
// Example configuration structure:
//
//	type Config struct {
//	  Username    string                 `configKey:"username" configUsage:"Database username."`
//	  Password    string                 `configKey:"password" configUsage:"Database password." sensitive:"true"`
//	  Tags        []string               `configKey:"tags" configUsage:"Optional tags."`
//	  Interactive configmap.Value[bool]  `configKey:"interactive"`
//	}
//
//	func (c Config) Normalize() {
//	  c.Username = strings.TrimSpace(c.Username)
//	}
//
//	func (c Config) Validate() error {
//	  if c.Username == "" {
//	    return errors.New("username is required")
//	  }
//	  return nil
//	}
//
// # Bind
//
// Use the [Bind] function to bind flags, environment variables and configuration files to the configuration structure.
//
//	// Get ENVs
//	envs, err := env.FromOs()
//	if err != nil {
//	  return err
//	}
//
//	// Define ENV naming
//	envNaming := env.NewNamingConvention("MY_APP_")
//
//	// Define default values
//	config := Config{
//	  Username: "default",
//	}
//
//	// Create bind specification
//	spec := configmap.BindSpec{
//	  Args: os.Args,
//	  Envs: envs,
//	  EnvNaming: envNaming,
//	  GenerateHelpFlag: true,
//	  GenerateConfigFileFlag: true,
//	  GenerateDumpConfigFlag: true,
//	}
//
//	// Bind
//	if err := configmap.Bind(spec, &config); err != nil {
//	  return err
//	}
//
// # Flags
//
// Example output of the --help flag.
//
//	Usage of "app":
//	    --config-file strings   Path to the configuration file.
//	    --dump-config string    Dump the effective configuration to STDOUT, "json" or "yaml".
//	    --help                  Print help message.
//	    --interactive
//	    --password string       Database password.
//	    --tags strings          Optional tags.
//	    --username string       Database username. (default "default")
//
//	Configuration source priority: 1. flag, 2. ENV, 3. config file
//
//	Flags can also be defined as ENV variables.
//	For example, the flag "--foo-bar" becomes the "MY_APP_FOO_BAR" ENV.
//
//	Use "--config-file" flag to specify a JSON/YAML configuration file, it can be used multiple times.
//
//	Use "--dump-config" flag with "json" or "yaml" value to dump configuration to STDOUT.
//
// # Environment Variables
//
// Each flag can also be specified as an environment variable.
//
//	export MY_APP_INTERACTIVE=
//	export MY_APP_PASSWORD=
//	export MY_APP_TAGS=
//	export MY_APP_USERNAME=
//
// # Config Files
//
// Use --config-file flag to specify a JSON/YAML configuration file, the flag can be used multiple times.
//
// Example configuration file can be generated by the --dump-config flag, see below.
//
// # Dump
//
// Example output of the --dump-config=yaml. Output can be used as a template of a configuration file.
//
//	# Database username.
//	username: default
//	# Database password.
//	password: '*****'
//	# Optional tags.
//	tags:
//	    - tag1
//	    - tag2
//	interactive: false
//
// # Normalization and Validation
//
// Configuration structure and each nested value inside it and marked with the "configKey" tag may:
//  - Implement the Normalize method, see ValueWithNormalization interface.
//  - Implement the Validate method, see ValueWithValidation interface.
//  - Has "validate" tag to define validation rules.
//
// # SetBy
//
// If you need to find out the source of the value, wrap the field type in the generic type Value[T].
//
//	type Config struct {
//	  ...
//	  Interactive configmap.Value[bool]  `configKey:"interactive"`
//	  ...
//	}
//
// In addition to the value itself, you can read the SetBy field.
//
//	config := Config{}
//	configmap.Bind(configmap.BindSpec{...}, &config)
//
//	// Print value
//	fmt.Println("interactive:", config.Interactive.Value)
//
//	// Test SetBy
//	if config.Interactive.SetBy == configmap.SetByEnv {
//	  ...
//	}
//
//	// Test IsSet
//	if config.Interactive.IsSet() {
//	  ...
//	}
package configmap

const (
	configKeyTag       = "configKey"
	configUsageTag     = "configUsage"
	sensitiveTag       = "sensitive"
	sensitiveMask      = "*****"
	tagValuesSeparator = ","
)
