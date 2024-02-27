package cmdconfig

import (
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

const (
	NonInteractiveOpt  = `non-interactive`
	StorageAPIHostOpt  = `storage-api-host`
	StorageAPITokenOpt = `storage-api-token`

	ENVPrefix = "KBC_"
)

type Binder struct {
	envNaming *env.NamingConvention
	envs      env.Provider
}

func NewBinder(envs env.Provider) *Binder {
	return &Binder{
		envNaming: env.NewNamingConvention(ENVPrefix),
		envs:      envs,
	}
}

func (b *Binder) Bind(flags *pflag.FlagSet, args []string, targets ...any) error {
	cfg := configmap.BindConfig{Flags: flags, Args: args, EnvNaming: b.envNaming, Envs: b.envs}
	return configmap.Bind(cfg, targets...)
}
