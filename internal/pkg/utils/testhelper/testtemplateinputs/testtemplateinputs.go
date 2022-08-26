package testtemplateinputs

import (
	"context"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testInputsEnvProvider struct {
	ctx  context.Context
	envs *env.Map
}

// CreateTestInputsEnvProvider allows you to generate new unique IDs via an ENV variable in the test.
func CreateTestInputsEnvProvider(ctx context.Context) (testhelper.EnvProvider, error) {
	allEnvs, err := env.FromOs()
	if err != nil {
		return nil, err
	}
	res := make(map[string]string)
	for k, v := range allEnvs.ToMap() {
		if strings.HasPrefix(k, "KAC_SECRET_") {
			res[k] = v
		}
	}
	return &testInputsEnvProvider{ctx: ctx, envs: env.FromMap(res)}, nil
}

func (p *testInputsEnvProvider) MustGet(key string) string {
	return p.envs.MustGet(key)
}
