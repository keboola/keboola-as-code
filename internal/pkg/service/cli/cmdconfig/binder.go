package cmdconfig

import (
	"context"
	"reflect"

	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
)

const (
	ENVPrefix = "KBC_"
)

type Binder struct {
	envNaming *env.NamingConvention
	envs      env.Provider
	logger    log.Logger
}

func NewBinder(envs env.Provider, l log.Logger) *Binder {
	return &Binder{
		envNaming: env.NewNamingConvention(ENVPrefix),
		envs:      envs,
		logger:    l,
	}
}

func (b *Binder) Bind(ctx context.Context, flags *pflag.FlagSet, args []string, targets ...any) error {
	cfg := configmap.BindConfig{Flags: flags, Args: args, EnvNaming: b.envNaming, Envs: b.envs}
	err := configmap.Bind(cfg, targets...)
	if err != nil {
		return err
	}

	for _, v := range targets {
		dump, err := configmap.NewDumper().Dump(v).AsJSON(false)
		if err == nil {
			b.logger.Debugf(ctx, "Global flags: %s %s", reflect.ValueOf(v).Type().String(), string(dump))
		} else {
			return err
		}
	}
	return nil
}
