package cliconfig

import (
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SetBy int

const (
	SetByUnknown SetBy = iota
	SetByFlag
	SetByFlagDefault
	SetByEnv
	SetManually
)

type ConfigStruct interface {
	Normalize()
	Validate() error
}

// LoadTo setups a config struct from CLI args and environment variables.
// Flags have priority over environment variables.
// The struct is normalized and validated.
func LoadTo(target ConfigStruct, args []string, envs env.Provider, envPrefix string) error {
	flags := pflag.NewFlagSet(args[0], pflag.ContinueOnError)
	if err := GenerateFlags(target, flags); err != nil {
		return errors.Errorf("cannot generate flags: %w", err)
	}
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			fmt.Println()
			fmt.Println(`Each flag can also be defined as an environment`)
			fmt.Printf(`variable with the "%s" prefix.`, envPrefix)
			fmt.Println()
			fmt.Println()
			fmt.Println(`For example, the flag "--foo-bar.baz" can be defined`)
			fmt.Printf(`as the "%sFOO_BAR_BAZ" environment variable.`, envPrefix)
			fmt.Println()
			fmt.Println()
			fmt.Println(`Flags take precedence over environment variables.`)
			fmt.Println()
		}
		return err
	}

	envNaming := env.NewNamingConvention(envPrefix)
	if err := BindToStruct(target, flags, envs, envNaming); err != nil {
		return err
	}

	return nil
}

// BindToStruct binds flags and environment variables to a config struct.
// Flags have priority over environment variables.
// The struct is normalized and validated.
func BindToStruct(target ConfigStruct, flags *pflag.FlagSet, envs env.Provider, envNaming *env.NamingConvention) error {
	v := viper.New()
	if _, err := BindToViper(v, flags, envs, envNaming); err != nil {
		return err
	}

	err := v.Unmarshal(target, viper.DecodeHook(unmarshallerHookFunc()), withZeroFields())
	if err != nil {
		return err
	}

	target.Normalize()
	if err := target.Validate(); err != nil {
		return err
	}
	return nil
}

// BindToViper binds flags and environment variables to a Viper instance.
// Flags have priority over environment variables.
func BindToViper(v *viper.Viper, flags *pflag.FlagSet, envs env.Provider, envNaming *env.NamingConvention) (map[string]SetBy, error) {
	// Bind flags
	if err := v.BindPFlags(flags); err != nil {
		return nil, err
	}

	// Search for ENV for each know flag
	setBy := make(map[string]SetBy)
	fromEnvs := make(map[string]any)
	flags.VisitAll(func(flag *pflag.Flag) {
		envName := envNaming.FlagToEnv(flag.Name)
		if flag.Changed {
			setBy[flag.Name] = SetByFlag
		} else if value, found := envs.Lookup(envName); found {
			fromEnvs[flag.Name] = value
			setBy[flag.Name] = SetByEnv
		} else {
			setBy[flag.Name] = SetByFlagDefault
		}
	})

	// Set fromEnvs map as a config, it has < priority as a flag,
	// so flag value is used if set, otherwise ENV is used.
	if err := v.MergeConfigMap(fromEnvs); err != nil {
		return nil, err
	}

	return setBy, nil
}

// unmarshallerHookFunc implements unmarshalling for custom types.
func unmarshallerHookFunc() mapstructure.DecodeHookFuncType {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if t.Kind() == reflect.Pointer {
			// Use nil value for pointer type, if the data is empty
			if reflect.ValueOf(data).IsZero() {
				return nil, nil
			}

			// Dereference pointer type
			t = t.Elem()
		}

		// Unmarshal string by an unmarshaler
		if f.Kind() == reflect.String {
			str := data.(string)
			bytes := []byte(str)
			switch v := reflect.New(t).Interface().(type) {
			case *time.Duration:
				if str != "" {
					return time.ParseDuration(str)
				}
			case encoding.TextUnmarshaler:
				if err := v.UnmarshalText(bytes); err != nil {
					return nil, err
				}
				return v, nil
			case encoding.BinaryUnmarshaler:
				if err := v.UnmarshalBinary(bytes); err != nil {
					return nil, err
				}
				return v, nil
			case json.Unmarshaler:
				if err := v.UnmarshalJSON(bytes); err != nil {
					return nil, err
				}
				return v, nil
			}
		}

		return data, nil
	}
}

// withZeroFields forces reset of each field before unmarshal.
func withZeroFields() viper.DecoderConfigOption {
	return func(config *mapstructure.DecoderConfig) {
		config.ZeroFields = true
	}
}
