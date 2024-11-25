package configmap

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	SetByDefault SetBy = iota
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

type BindConfig struct {
	Flags       *pflag.FlagSet        // required
	Args        []string              // optional
	ConfigFiles []string              // optional
	EnvNaming   *env.NamingConvention // optional
	Envs        env.Provider          // optional
}

type GenerateAndBindConfig struct {
	Args                   []string              // optional
	PositionalArgsTarget   *[]string             // optional
	ConfigFiles            []string              // optional
	EnvNaming              *env.NamingConvention // optional
	Envs                   env.Provider          // optional
	GenerateHelpFlag       bool
	GenerateConfigFileFlag bool
	GenerateDumpConfigFlag bool
}

// ValueWithNormalization is a nested value with the Normalize method that is called on Bind and BindToViper.
type ValueWithNormalization interface {
	Normalize()
}

// ValueWithValidation is a nested value with the Validate method that is called on Bind and BindToViper.
type ValueWithValidation interface {
	Validate() error
}

// FlagToFieldFn translates flag definition to the field name in the configuration.
type FlagToFieldFn func(flag *pflag.Flag) (orderedmap.Path, bool)

// GenerateAndBind generates flags and then bind flags, ENVs and config files to target configuration structures.
// This is an all-in-one solution.
// If you want to use the Cobra CLI framework, you will need the GenerateFlags and Bind functions.
func GenerateAndBind(cfg GenerateAndBindConfig, targets ...any) error {
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
	for _, target := range targets {
		if err := GenerateFlags(flags, target); err != nil {
			return err
		}
	}

	// Parse flags
	if err := flags.Parse(cfg.Args); err != nil {
		return errors.Errorf(`cannot parse flags: %w`, err)
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
	flagToFieldMap, err := newFlagToFieldMap(targets...)
	if err != nil {
		return err
	}
	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		v, ok := flagToFieldMap[flag.Name]
		return v, ok
	}

	// Bind config files, flags, envs to the structs
	errs := errors.NewMultiError()
	for _, target := range targets {
		// Validate type
		if v := reflect.ValueOf(target); v.Kind() != reflect.Pointer && v.Type().Elem().Kind() != reflect.Pointer {
			return errors.Errorf(`cannot bind configuration to the "%s": expected a pointer to a struct`, v.Type().String())
		}

		bindCfg := BindConfig{Flags: flags, Args: cfg.Args, ConfigFiles: configFiles, EnvNaming: cfg.EnvNaming, Envs: cfg.Envs}
		if err := bind(bindCfg, target, flagToField); err != nil {
			errs.Append(err)
		}
	}

	// Stop on bind errors
	if errs.Len() > 0 {
		return errs.ErrorOrNil()
	}

	// Validate
	for _, target := range targets {
		if err := ValidateAndNormalize(target); err != nil {
			errs.Append(err)
		}
	}

	// Dump config
	if dumpConfigFlag != nil && *dumpConfigFlag != "" {
		d := NewDumper()
		for _, target := range targets {
			d.Dump(target)
		}
		if bytes, err := d.As(*dumpConfigFlag); err == nil {
			return DumpError{Dump: bytes, ValidationError: errs.ErrorOrNil()}
		} else {
			return err
		}
	}

	// Save remaining - positional arguments
	if cfg.PositionalArgsTarget != nil {
		*cfg.PositionalArgsTarget = flags.Args()
	}

	return errs.ErrorOrNil()
}

// Bind flags, ENVs and config files to target configuration structures.
func Bind(inputs BindConfig, targets ...any) error {
	if len(targets) == 0 {
		return errors.Errorf(`at least one ConfigStruct must be provided`)
	}
	if inputs.Flags == nil {
		return errors.Errorf(`flags must be specified`)
	}

	// Parse flags
	if err := inputs.Flags.Parse(inputs.Args); err != nil {
		return errors.Errorf(`cannot parse flags: %w`, err)
	}

	// Define mapping between flag and field path
	flagToFieldMap, err := newFlagToFieldMap(targets...)
	if err != nil {
		return err
	}
	flagToField := func(flag *pflag.Flag) (orderedmap.Path, bool) {
		v, ok := flagToFieldMap[flag.Name]
		return v, ok
	}

	// Bind config files, flags, envs to the structs
	errs := errors.NewMultiError()
	for _, target := range targets {
		// Validate type
		if v := reflect.ValueOf(target); v.Kind() != reflect.Pointer || v.Type().Elem().Kind() != reflect.Struct {
			return errors.Errorf(`cannot bind to type "%s": expected a pointer to a struct`, v.Type().String())
		}

		if err := bind(inputs, target, flagToField); err != nil {
			errs.Append(err)
		}
	}

	// Validate
	for _, target := range targets {
		if err := ValidateAndNormalize(target); err != nil {
			errs.Append(err)
		}
	}

	return errs.ErrorOrNil()
}

func ValidateAndNormalize(target any) error {
	// Call Normalize and Validate methods on each value
	validationErrs := errors.NewMultiError()
	_ = Visit(reflect.ValueOf(target), VisitConfig{
		OnField: mapAndFilterField(),
		OnValue: func(vc *VisitContext) error {
			// Get pointer to the value, methods may be defined on the pointer
			value := vc.Value
			if value.Kind() != reflect.Pointer && value.CanAddr() {
				value = value.Addr()
			}

			// Invalid value may happen if a Normalize call replaces a value with nil before it can be visited
			if value.Kind() == reflect.Invalid {
				return nil
			}

			// Call Normalize method, if any
			if v, ok := value.Interface().(ValueWithNormalization); ok {
				v.Normalize()
			}

			// Call Validate method, if any
			if v, ok := value.Interface().(ValueWithValidation); ok {
				if err := v.Validate(); err != nil {
					if path := vc.MappedPath.String(); path == "" {
						validationErrs.Append(err)
					} else {
						validationErrs.Append(errors.Errorf(`invalid "%s": %w`, path, err))
					}
				}
			}

			return nil
		},
	})

	// Validate with validator
	if err := validator.New().Validate(context.Background(), target); err != nil {
		validationErrs.Append(err)
	}

	if err := validationErrs.ErrorOrNil(); err != nil {
		return errors.PrefixError(err, "configuration is not valid")
	}

	return nil
}

// bind binds flags, ENVs and config files to the configuration structure.
// Flags have priority over environment variables.
// The struct is normalized and validated.
func bind(inputs BindConfig, target any, flagToFieldFn FlagToFieldFn) error {
	values, err := collectValues(inputs, flagToFieldFn)
	if err != nil {
		return errors.PrefixError(err, "value error")
	}

	// Decode
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

	if decoder, err := mapstructure.NewDecoder(decoderCfg); err != nil {
		return errors.PrefixError(err, "cannot create decoder")
	} else if err := decoder.Decode(values); err != nil {
		return errors.PrefixError(err, "decode error")
	}

	return nil
}

// collectValues defined in the configuration structure from flags, ENVs and config files.
// Priority: 1. flag, 2. ENV, 3. config file.
func collectValues(cfg BindConfig, flagToField FlagToFieldFn) (*orderedmap.OrderedMap, error) {
	errs := errors.NewMultiError()
	values := orderedmap.New()

	// Flags default values
	cfg.Flags.VisitAll(func(flag *pflag.Flag) {
		if fieldPath, ok := flagToField(flag); ok {
			if !flag.Changed {
				if err := values.SetNestedPath(fieldPath, fieldValue{Value: flag.Value.String(), SetBy: SetByDefault}); err != nil {
					errs.Append(err)
				}
			}
		}
	})

	// Parse configuration files
	for _, path := range cfg.ConfigFiles {
		// Read
		content, err := os.ReadFile(path) //nolint: forbidigo
		if err != nil {
			errs.Append(errors.Errorf(`cannot read config file "%s": %w`, path, err))
			continue
		}

		// Decode
		config := orderedmap.New()
		ext := strings.ToLower(strings.TrimLeft(filepath.Ext(path), ".")) //nolint: forbidigo
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
			// Copy leaf values = value is not object AND key is map step, not slice step
			_, isObject := v.(*orderedmap.OrderedMap)
			_, isMapKey := path.Last().(orderedmap.MapStep)
			if !isObject && isMapKey {
				if err := values.SetNestedPath(path, fieldValue{Value: v, SetBy: SetByConfig}); err != nil {
					errs.Append(err)
				}
			}
		})
	}

	// Bind flags and ENVs, flags have priority
	cfg.Flags.VisitAll(func(flag *pflag.Flag) {
		if fieldPath, ok := flagToField(flag); ok {
			if flag.Changed {
				if err := values.SetNestedPath(fieldPath, fieldValue{Value: flag.Value.String(), SetBy: SetByFlag}); err != nil {
					errs.Append(err)
				}
			} else if cfg.EnvNaming != nil && cfg.Envs != nil {
				envName := cfg.EnvNaming.FlagToEnv(flag.Name)
				if envValue, found := cfg.Envs.Lookup(envName); found {
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

		// Handle text
		if from.Kind() == reflect.String {
			str, ok := from.Interface().(string)
			if !ok {
				// value is not string
				return nil, errors.Errorf(`expected string, got "%s"`, from.String())
			}

			err := UnmarshalText([]byte(str), to)
			switch {
			case errors.As(err, &NoTextTypeError{}):
				// continue, no unmarshaler found
			case err != nil:
				// unmarshaler found, but an error occurred
				return nil, err
			case to.Kind() == reflect.Pointer && to.IsNil():
				// left the field nil, mapstructure library requires any(nil) in this case
				return nil, nil
			default:
				// ok
				return to.Interface(), nil
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

		// Fallback
		return from.Interface(), nil
	}
}
