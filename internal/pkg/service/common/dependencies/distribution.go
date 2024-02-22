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

func NewDistributionScope(ctx context.Context, nodeID string, d distributionScopeDeps) (DistributionScope, error) {
	return newDistributionScope(ctx, nodeID, d)
}

func newDistributionScope(ctx context.Context, nodeID string, d distributionScopeDeps) (v *distributionScope, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.common.dependencies.NewDistributionScope")
	defer span.End(&err)

	node := distributionPkg.NewNode(nodeID, distributionPkg.NewConfig(), d)

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
