package service

import (
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	dependencies "github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
)

type service struct{}

func New() Service {
	return &service{}
}

// APIRootIndex implements buffer.Service
func (*service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

// APIVersionIndex implements buffer.Service
func (*service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

// HealthCheck implements buffer.Service
func (*service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}

// CreateReceiver implements buffer.Service
func (*service) CreateReceiver(dependencies.ForProjectRequest, *buffer.CreateReceiverPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// GetReceiver implements buffer.Service
func (*service) GetReceiver(dependencies.ForProjectRequest, *buffer.GetReceiverPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// ListReceivers implements buffer.Service
func (*service) ListReceivers(dependencies.ForProjectRequest, *buffer.ListReceiversPayload) (res []*buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// DeleteReceiver implements buffer.Service
func (*service) DeleteReceiver(dependencies.ForProjectRequest, *buffer.DeleteReceiverPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// RefreshReceiverTokens implements buffer.Service
func (*service) RefreshReceiverTokens(dependencies.ForProjectRequest, *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// CreateExport implements buffer.Service
func (*service) CreateExport(dependencies.ForProjectRequest, *buffer.CreateExportPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// UpdateExport implements buffer.Service
func (*service) UpdateExport(dependencies.ForProjectRequest, *buffer.UpdateExportPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// DeleteExport implements buffer.Service
func (*service) DeleteExport(dependencies.ForProjectRequest, *buffer.DeleteExportPayload) (res *buffer.Receiver, err error) {
	return nil, fmt.Errorf("not yet implemented")
}

// Import implements buffer.Service
func (*service) Import(dependencies.ForPublicRequest, *buffer.ImportPayload, io.ReadCloser) (err error) {
	return fmt.Errorf("not yet implemented")
}
