package testtemplateinputs

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
)

type TestInputsEnvProvider struct {
	ctx  context.Context
	envs *env.Map
}

// CreateTestInputsEnvProvider allows you to inject only ENV variables with correct prefix.
func CreateTestInputsEnvProvider(ctx context.Context) (*TestInputsEnvProvider, error) {
	allEnvs, err := env.FromOs()
	if err != nil {
		return nil, err
	}
	return &TestInputsEnvProvider{ctx: ctx, envs: allEnvs}, nil
}

func (p *TestInputsEnvProvider) MustGet(key string) string {
	return p.envs.MustGet(key)
}

func (p *TestInputsEnvProvider) Get(key string) (string, error) {
	val := p.envs.Get(key)
	if len(val) == 0 {
		return "", fmt.Errorf("missing ENV variable \"%s\"", strings.ToUpper(key))
	}
	return val, nil
}
