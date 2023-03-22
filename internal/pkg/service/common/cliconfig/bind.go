package cliconfig

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

type SetBy int

const (
	SetByUnknown SetBy = iota
	SetByFlag
	SetByFlagDefault
	SetByEnv
	SetManually
)

// BindFlagsAndEnvToStruct binds flags and environment variables to a config struct.
// Flags have priority over environment variables.
func BindFlagsAndEnvToStruct(target any, flags *pflag.FlagSet, envs env.Provider, envNaming *env.NamingConvention) error {
	v := viper.New()
	if _, err := BindFlagsAndEnvToViper(v, flags, envs, envNaming); err != nil {
		return err
	}
	if err := v.Unmarshal(target); err != nil {
		return err
	}
	return nil
}

// BindFlagsAndEnvToViper binds flags and environment variables to a Viper instance.
// Flags have priority over environment variables.
func BindFlagsAndEnvToViper(v *viper.Viper, flags *pflag.FlagSet, envs env.Provider, envNaming *env.NamingConvention) (map[string]SetBy, error) {
	// Bind flags
	if err := v.BindPFlags(flags); err != nil {
		return nil, err
	}

	// Search for ENV for each know flag
	setBy := make(map[string]SetBy)
	fromEnvs := make(map[string]interface{})
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
