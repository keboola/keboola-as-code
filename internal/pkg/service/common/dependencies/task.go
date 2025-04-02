package dependencies

import (
	"context"

	taskPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/task"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// taskScope implements TaskScope interface.
type taskScope struct {
	node *taskPkg.Node
}

type taskNodeDeps struct {
	BaseScope
	EtcdClientScope
	DistributionScope
}

func NewTaskScope(ctx context.Context, nodeID string, exceptionIDPrefix string, baseScp BaseScope, etcdScp EtcdClientScope, distScp DistributionScope, cfg taskPkg.NodeConfig) (TaskScope, error) {
	return newTaskScope(ctx, nodeID, exceptionIDPrefix, baseScp, etcdScp, distScp, cfg)
}

func newTaskScope(ctx context.Context, nodeID string, exceptionIDPrefix string, baseScp BaseScope, etcdScp EtcdClientScope, distScp DistributionScope, cfg taskPkg.NodeConfig) (v *taskScope, err error) {
	_, span := baseScp.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewTaskScope")
	defer span.End(&err)

	d := taskNodeDeps{BaseScope: baseScp, EtcdClientScope: etcdScp, DistributionScope: distScp}

	node, err := taskPkg.NewNode(nodeID, exceptionIDPrefix, d, cfg)
	if err != nil {
		return nil, err
	}

	return &taskScope{node: node}, nil
}

func (v *taskScope) check() {
	if v == nil {
		panic(errors.New("dependencies task scope is not initialized"))
	}
}

func (v *taskScope) TaskNode() *taskPkg.Node {
	v.check()
	return v.node
}
