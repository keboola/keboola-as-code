package configmap

import (
	"encoding"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	SetByUnknown SetBy = iota
	SetByDefault
	SetByConfig
	SetByFlag
	SetByEnv
	SetManually     // deprecated
	HelpFlag        = "help"
	ConfigFileFlag  = "config-file"
	DumpConfigFlag  = "dump-config"
	HelpUsage       = "Print help message."
	ConfigFileUsage = "Path to the configuration file."
	DumpConfigUsage = "Dump the effective configuration to STDOUT, \"json\" or \"yaml\"."
	SliceSeparator  = ","
)

type SetBy int

// BindSpec is a configuration for the Bind function.
type BindSpec struct {
	Args                   []string
	EnvNaming              *env.NamingConvention
	Envs                   env.Provider
	GenerateHelpFlag       bool
	GenerateConfigFileFlag bool
	GenerateDumpConfigFlag bool
}

// FlagToFieldFn translates flag definition to the field name in the configuration.
type FlagToFieldFn func(flag *pflag.Flag) (orderedmap.Path, bool)

// ConfigStruct of a service.
type ConfigStruct interface {
	Normalize()
	Validate() error
}

// Bind flags, ENVs and config files to target configuration structures.
func Bind(cfg BindSpec, targets ...ConfigStruct) error {
	if len(targets) == 0 {
		return errors.Errorf(`at least one ConfigStruct must be provided`)
	}

	// Get app name
	appName := ""
	if len(cfg.Args) > 0 {
		appName = cfg.Args[0]
	}

	// Generate common flags
	flags := pflag.NewFlagSet(appName, pflag.ContinueOnError)
	var helpFlag *bool
	if cfg.GenerateHelpFlag {
		helpFlag = flags.BoolP(HelpFlag, "", false, HelpUsage)
	}
	var configFilesFlag *[]string
	if cfg.GenerateConfigFileFlag {
		configFilesFlag = flags.StringSliceP(ConfigFileFlag, "", nil, ConfigFileUsage)
	}
	var dumpConfigFlag *string
	if cfg.GenerateDumpConfigFlag {
		dumpConfigFlag = flags.StringP(DumpConfigFlag, "", "", DumpConfigUsage)
	}

	// Generate flags from the structs
	flagToFieldMap := make(map[string]orderedmap.Path)
	for _, target := range targets {
		if err := StructToFlags(flags, target, flagToFieldMap); err != nil {
			return err
		}
	}

	// Parse flags
	if err := flags.Parse(cfg.Args); err != nil {
		return err
	}

	// Return help
	if helpFlag != nil && *helpFlag {
		return newHelpError(cfg.Args[0], flags, cfg)
	}

	// Merge config file paths defined by flags and ENVs
	var configFiles []string
	if configFilesFlag != nil {
		configFiles = append(configFiles, *configFilesFlag...)
		if cfg.EnvNaming != nil && cfg.Envs != nil {
			envValue := cfg.EnvNaming.FlagToEnv(ConfigFileFlag)
			if v, _ := cfg.Envs.Lookup(envValue); v != "" {
				configFiles = append(configFiles, v)
			}
		}
	}

	// Define mapping between flag and field path
	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		v, ok := flagToFieldMap[flag.Name]
		return v, ok
	}

	// Bind config files, flags, envs to the structs
	for _, target := range targets {
		if err := bind(cfg, target, flags, flagToField, configFiles); err != nil {
			return err
		}
	}

	// Normalize and validate the structs
	errs := errors.NewMultiError()
	for _, target := range targets {
		target.Normalize()
		if err := target.Validate(); err != nil {
			errs.Append(err)
		}
	}

	// Dump config
	if dumpConfigFlag != nil && *dumpConfigFlag != "" && errs.Len() == 0 {
		d := NewDumper()
		for _, target := range targets {
			d.Dump(target)
		}
		if bytes, err := d.As(*dumpConfigFlag); err == nil {
			return DumpError{Dump: bytes}
		} else {
			return err
		}
	}

	return errs.ErrorOrNil()
}

// bind binds flags, ENVs and config files to the configuration structure.
// Flags have priority over environment variables.
// The struct is normalized and validated.
func bind(cfg BindSpec, target ConfigStruct, flags *pflag.FlagSet, flagToFieldFn FlagToFieldFn, configFiles []string) error {
	values, err := collectValues(flags, flagToFieldFn, cfg.EnvNaming, cfg.Envs, configFiles)
	if err != nil {
		return err
	}

	decoderCfg := &mapstructure.DecoderConfig{
		TagName:          configKeyTag,
		ZeroFields:       true,
		WeaklyTypedInput: true,
		Result:           target,
	}

	decoderCfg.DecodeHook = mapstructure.ComposeDecodeHookFunc(
		unmarshalHook(&decoderCfg.DecodeHook),
		// additional hooks can be added
	)

	decoder, err := mapstructure.NewDecoder(decoderCfg)
	if err != nil {
		return err
	}

	if err := decoder.Decode(values); err != nil {
		return err
	}

	target.Normalize()

	if err := target.Validate(); err != nil {
		return err
	}

	return nil
}

// collectValues defined in the configuration structure from flags, ENVs and config files.
// Priority: 1. flag, 2. ENV, 3. config file.
func collectValues(flags *pflag.FlagSet, flagToField FlagToFieldFn, envNaming *env.NamingConvention, envs env.Provider, configFiles []string) (*orderedmap.OrderedMap, error) {
	errs := errors.NewMultiError()
	values := orderedmap.New()

	// Flags default values
	flags.VisitAll(func(flag *pflag.Flag) {
		if fieldPath, ok := flagToField(flag); ok {
			if !flag.Changed {
				if err := values.SetNestedPath(fieldPath, fieldValue{Value: flag.Value.String(), SetBy: SetByDefault}); err != nil {
					errs.Append(err)
				}
			}
		}
	})

	// Parse configuration files
	for _, path := range configFiles {
		// Read
		content, err := os.ReadFile(path)
		if err != nil {
			errs.Append(errors.Errorf(`cannot read config file "%s": %w`, path, err))
			continue
		}

		// Decode
		config := orderedmap.New()
		ext := strings.ToLower(strings.TrimLeft(filepath.Ext(path), "."))
		switch ext {
		case "json":
			if err := json.Unmarshal(content, config); err != nil {
				errs.Append(errors.Errorf(`cannot decode JSON config file "%s": %w`, path, err))
				continue
			}
		case "yml", "yaml":
			if err := yaml.Unmarshal(content, config); err != nil {
				errs.Append(errors.Errorf(`cannot decode YAML config file "%s": %w`, path, err))
				continue
			}
		default:
			errs.Append(errors.Errorf(`unexpected format "%s" of the config file "%s": expected one of "yml", "yaml", "json"`, ext, path))
			continue
		}

		// Store
		config.VisitAllRecursive(func(path orderedmap.Path, v any, parent any) {
			// Copy leaf values
			if _, ok := v.(*orderedmap.OrderedMap); !ok {
				if err := values.SetNestedPath(path, fieldValue{Value: v, SetBy: SetByConfig}); err != nil {
					errs.Append(err)
				}
			}
		})
	}

	// Bind flags and ENVs, flags have priority
	flags.VisitAll(func(flag *pflag.Flag) {
		if fieldPath, ok := flagToField(flag); ok {
			if flag.Changed {
				if err := values.SetNestedPath(fieldPath, fieldValue{Value: flag.Value.String(), SetBy: SetByFlag}); err != nil {
					errs.Append(err)
				}
			} else if envNaming != nil && envs != nil {
				envName := envNaming.FlagToEnv(flag.Name)
				if envValue, found := envs.Lookup(envName); found {
					if err := values.SetNestedPath(fieldPath, fieldValue{Value: envValue, SetBy: SetByEnv}); err != nil {
						errs.Append(err)
					}
				}
			}
		}
	})

	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return values, nil
}

// unmarshalHook implements unmarshalling for complex types.
func unmarshalHook(hooks *mapstructure.DecodeHookFunc) mapstructure.DecodeHookFuncValue {
	return func(from reflect.Value, to reflect.Value) (any, error) {
		// Unwrap fieldValue
		if v, ok := from.Interface().(fieldValue); ok {
			// Get wrapped Value and recursive apply all hooks
			out, err := mapstructure.DecodeHookExec(*hooks, reflect.ValueOf(v.Value), to)
			if err != nil {
				return nil, err
			} else if container, ok := out.(withOrigin); ok {
				container.SetOrigin(v.SetBy)
			}
			return out, nil
		}

		// Handle OrderedMap
		if v, ok := from.Interface().(*orderedmap.OrderedMap); ok {
			return v.ToMap(), nil
		}

		// Handle a value that implements ValueContainer interface
		if v, ok := to.Addr().Interface().(ValueContainer); ok {
			internalType := v.ValueType()
			internalValue := reflect.New(internalType).Elem()
			if internalValueRaw, err := mapstructure.DecodeHookExec(*hooks, from, internalValue); err != nil {
				return nil, fmt.Errorf(`cannot set "%s" to "%s": %w`, internalType.String(), to.Type().String(), err)
			} else if err = mapstructure.WeakDecode(internalValueRaw, internalValue.Addr().Interface()); err != nil {
				return nil, fmt.Errorf(`cannot set "%s" to "%s": %w`, internalType.String(), to.Type().String(), err)
			} else if !v.TrySetValue(internalValue) {
				return nil, fmt.Errorf(`cannot set "%s" to "%s"`, internalType.String(), to.Type().String())
			} else {
				return v, nil
			}
		}

		// Handle slice
		if to.Kind() == reflect.Slice {
			if str, ok := from.Interface().(string); ok {
				// Remove surrounding [...] from []string slice represented as a string
				str = strings.TrimSuffix(strings.TrimPrefix(str, "["), "]")

				// Split slice parts
				if str == "" {
					return []string(nil), nil
				} else {
					return strings.Split(str, SliceSeparator), nil
				}
			}
		}

		// Map string to a type
		if from.Kind() == reflect.String {
			str, ok := from.Interface().(string)
			if !ok {
				return nil, errors.Errorf(`expected string, got "%s"`, from.String())
			}

			// Handle empty string as nil pointer
			if str == "" && to.Kind() == reflect.Pointer {
				return nil, nil
			}

			// Get pointer to the value, unmarshal method may be defined on the pointer
			var toPtr reflect.Value
			if to.Kind() == reflect.Pointer {
				to = reflect.New(to.Type().Elem())
				toPtr = to
			} else {
				toPtr = to.Addr()
			}

			// Unmarshal string by an unmarshaler
			switch v := toPtr.Interface().(type) {
			case *time.Duration:
				if str != "" {
					var err error
					if *v, err = time.ParseDuration(str); err != nil {
						return nil, err
					}
				}
				return to.Interface(), nil
			case encoding.TextUnmarshaler:
				if err := v.UnmarshalText([]byte(str)); err != nil {
					return nil, err
				}
				return to.Interface(), nil
			case encoding.BinaryUnmarshaler:
				if err := v.UnmarshalBinary([]byte(str)); err != nil {
					return nil, err
				}
				return to.Interface(), nil
			case json.Unmarshaler:
				if err := v.UnmarshalJSON([]byte(str)); err != nil {
					return nil, err
				}
				return to.Interface(), nil
			}
		}

		// Fallback
		return from.Interface(), nil
	}
}
