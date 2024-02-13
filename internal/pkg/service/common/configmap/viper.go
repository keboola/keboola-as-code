package configmap

import (
	"github.com/keboola/go-utils/pkg/orderedmap"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// BindToViper flags, ENVs and config files to the Viper configuration registry.
// It is recommended to use the Bind instead of the BindToViper method if you have a choice,
// as the configuration structure is easier to use, both in production code and tests.
func BindToViper(viper *viper.Viper, flagToField FlagToFieldFn, cfg BindConfig) (map[string]SetBy, error) {
	errs := errors.NewMultiError()

	// Collect values from flags, ENVs and config files
	values, err := collectValues(cfg, flagToField)
	if err != nil {
		return nil, err
	}

	// Set default values
	// viper.SetDefault method cannot be used, IsSet returns always true.
	cfg.Flags.VisitAll(func(flag *pflag.Flag) {
		if !flag.Changed {
			if path, ok := flagToField(flag); ok {
				if err := viper.BindPFlag(path.String(), flag); err != nil {
					errs.Append(err)
				}
			}
		}
	})

	// Set values
	setBy := make(map[string]SetBy)
	values.VisitAllRecursive(func(path orderedmap.Path, value any, parent any) {
		if v, ok := value.(fieldValue); ok {
			key := path.String()
			setBy[key] = v.SetBy
			if v.SetBy != SetByDefault {
				viper.Set(key, v.Value)
			}
		}
	})

	if err := errs.ErrorOrNil(); err != nil {
		return nil, err
	}

	return setBy, nil
}
