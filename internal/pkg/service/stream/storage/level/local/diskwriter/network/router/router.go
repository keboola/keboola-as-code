package router

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
)

type Router struct {
	distribution *distribution.GroupNode
	connections  *connection.Manager
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
	DistributionNode() *distribution.Node
}

func New(d dependencies, sourceType string, cfg network.Config) (*Router, error) {
	r := &Router{}

	var err error

	// Join a distribution group, it contains all source nodes of the same type
	r.distribution, err = d.DistributionNode().Group("storage.router.sources." + sourceType)
	if err != nil {
		return nil, err
	}

	// Create connections to all disk writer nodes
	r.connections, err = connection.NewManager(d, cfg, r.distribution.NodeID())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Router) OpenPipeline(ctx context.Context, sinKey key.SinkKey) (pipeline.Pipeline, error) {
	return nil, svcErrors.NewNotImplementedError()
}
