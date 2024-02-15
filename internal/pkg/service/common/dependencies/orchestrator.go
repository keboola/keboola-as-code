package dependencies

import (
	"context"

	orchestratorPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type orchestratorScope struct {
	node *orchestratorPkg.Node
}

type orchestratorScopeDeps interface {
	BaseScope
	EtcdClientScope
	TaskScope
}

func NewOrchestratorScope(ctx context.Context, d orchestratorScopeDeps) OrchestratorScope {
	return newOrchestratorScope(ctx, d)
}

func newOrchestratorScope(ctx context.Context, d orchestratorScopeDeps) *orchestratorScope {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewOrchestratorScope")
	defer span.End(nil)
	return &orchestratorScope{node: orchestratorPkg.NewNode(d)}
}

func (v *orchestratorScope) check() {
	if v == nil {
		panic(errors.New("dependencies orchestrator scope is not initialized"))
	}
}

func (v *orchestratorScope) OrchestratorNode() *orchestratorPkg.Node {
	v.check()
	return v.node
}
