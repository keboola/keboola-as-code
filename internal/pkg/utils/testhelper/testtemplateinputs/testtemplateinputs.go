package testtemplateinputs

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type EnvProvider struct {
	ctx  context.Context
	envs *env.Map
}

// CreateTestInputsEnvProvider allows you to inject only ENV variables with correct prefix.
func CreateTestInputsEnvProvider(ctx context.Context) (*EnvProvider, error) {
	allEnvs, err := env.FromOs()
	if err != nil {
		return nil, err
	}
	return &EnvProvider{ctx: ctx, envs: allEnvs}, nil
}

func (p *EnvProvider) MustGet(key string) string {
	return p.envs.MustGet(key)
}

func (p *EnvProvider) Get(key string) (string, error) {
	val := p.envs.Get(key)
	if len(val) == 0 {
		return "", errors.Errorf("missing ENV variable \"%s\"", strings.ToUpper(key))
	}
	return val, nil
}
