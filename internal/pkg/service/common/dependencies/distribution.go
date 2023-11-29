package dependencies

import (
	"context"

	distributionPkg "github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// distributionScope implements DistributionScope interface.
type distributionScope struct {
	node *distributionPkg.Node
}

type distributionScopeDeps interface {
	BaseScope
	EtcdClientScope
	TaskScope
}

func NewDistributionScope(ctx context.Context, nodeID, group string, d distributionScopeDeps, opts ...distributionPkg.NodeOption) (DistributionScope, error) {
	return newDistributionScope(ctx, nodeID, group, d, opts...)
}

func newDistributionScope(ctx context.Context, nodeID, group string, d distributionScopeDeps, opts ...distributionPkg.NodeOption) (v *distributionScope, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewDistributionScope")
	defer span.End(&err)

	node, err := distributionPkg.NewNode(nodeID, group, d, opts...)
	if err != nil {
		return nil, err
	}

	return &distributionScope{node: node}, nil
}

func (v *distributionScope) check() {
	if v == nil {
		panic(errors.New("dependencies distribution scope is not initialized"))
	}
}

func (v *distributionScope) DistributionNode() *distributionPkg.Node {
	v.check()
	return v.node
}
