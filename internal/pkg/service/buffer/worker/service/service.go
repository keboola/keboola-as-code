package service

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/upload"
)

type Service struct {
	uploader *upload.Uploader
}

func New(d dependencies.ForWorker) (*Service, error) {
	uploader, err := upload.NewUploader(d)
	if err != nil {
		return nil, err
	}
	return &Service{uploader: uploader}, nil
}
