package dependencies

import (
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
}

func NewDistributionScope(nodeID string, cfg distributionPkg.Config, d distributionScopeDeps) DistributionScope {
	return newDistributionScope(nodeID, cfg, d)
}

func newDistributionScope(nodeID string, cfg distributionPkg.Config, d distributionScopeDeps) (v *distributionScope) {
	return &distributionScope{node: distributionPkg.NewNode(nodeID, cfg, d)}
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
