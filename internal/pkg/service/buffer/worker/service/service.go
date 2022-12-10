package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
)

type Service struct {
	dependencies.ForWorker
	dist *distribution.Node
}

func New(d dependencies.ForWorker) (*Service, error) {
	dist, err := distribution.NewNode(d)
	if err != nil {
		return nil, err
	}
	return &Service{ForWorker: d, dist: dist}, nil
}
