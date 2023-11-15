package configmap

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

// BindToViper flags, ENVs and config files to the Viper configuration registry.
// It is recommended to use the Bind instead of the BindToViper method if you have a choice,
// as the configuration structure is easier to use, both in production code and tests.
func BindToViper(viper *viper.Viper, flags *pflag.FlagSet, flagToField FlagToFieldFn, envs env.Provider, envNaming *env.NamingConvention, configFiles []string) (map[string]SetBy, error) {
	// Collect values from flags, ENVs and config files
	values, err := collectValues(flags, flagToField, envNaming, envs, configFiles)
	if err != nil {
		return nil, err
	}

	setBy := make(map[string]SetBy)
	values.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		if v, ok := value.(fieldValue); ok {
			key := path.String()
			setBy[key] = v.SetBy
			if v.SetBy == SetByDefault {
				viper.SetDefault(key, v.Value)
			} else {
				viper.Set(key, v.Value)
			}
		}
	})

	return setBy, nil
}
