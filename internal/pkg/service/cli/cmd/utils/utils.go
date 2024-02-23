package utils

import (
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

func GetBindConfig(flags *pflag.FlagSet, args []string) configmap.BindConfig {
	return configmap.BindConfig{
		Flags:     flags,
		Args:      args,
		EnvNaming: env.NewNamingConvention("KBC_"),
		Envs:      env.Empty(),
	}
}
