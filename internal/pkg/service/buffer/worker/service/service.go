package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/dependencies"
)

type Service struct {
	dependencies.ForWorker
}

func New(d dependencies.ForWorker) *Service {
	return &Service{ForWorker: d}
}

func (s *Service) Start() {
	s.Logger().Info("starting worker goroutines")
}
