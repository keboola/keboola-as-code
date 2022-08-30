package testtemplateinputs

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testInputsEnvProvider struct {
	ctx  context.Context
	envs *env.Map
}

// CreateTestInputsEnvProvider allows you to inject only ENV variables with correct prefix.
func CreateTestInputsEnvProvider(ctx context.Context) (testhelper.EnvProvider, error) {
	allEnvs, err := env.FromOs()
	if err != nil {
		return nil, err
	}
	return &testInputsEnvProvider{ctx: ctx, envs: allEnvs}, nil
}

func (p *testInputsEnvProvider) MustGet(key string) string {
	return p.envs.MustGet(key)
}
