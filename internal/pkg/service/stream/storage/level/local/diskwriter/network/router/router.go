package router

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	svcErrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
)

type Router struct {
	connections *connection.Manager
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
}

func New(d dependencies, cfg network.Config, nodeID string) (*Router, error) {
	r := &Router{}

	var err error

	r.connections, err = connection.NewManager(d, cfg, nodeID)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *Router) OpenPipeline(ctx context.Context, sinkType definition.SinkType) (pipeline.Pipeline, error) {
	return nil, svcErrors.NewNotImplementedError()
}
