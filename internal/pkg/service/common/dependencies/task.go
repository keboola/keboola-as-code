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

type taskScopeDependencies interface {
	BaseScope
	EtcdClientScope
}

func NewTaskScope(ctx context.Context, nodeID string, exceptionIDPrefix string, d taskScopeDependencies, opts ...taskPkg.NodeOption) (TaskScope, error) {
	return newTaskScope(ctx, nodeID, exceptionIDPrefix, d, opts...)
}

func newTaskScope(ctx context.Context, nodeID string, exceptionIDPrefix string, d taskScopeDependencies, opts ...taskPkg.NodeOption) (v *taskScope, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewTaskScope")
	defer span.End(&err)

	node, err := taskPkg.NewNode(nodeID, exceptionIDPrefix, d, opts...)
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
