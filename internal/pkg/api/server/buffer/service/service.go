package service

import (
	"fmt"
	"io"

	dependencies "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/common/service"
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type service struct{}

func New() Service {
	return &service{}
}

func (*service) APIRootIndex(dependencies.ForPublicRequest) (err error) {
	// Redirect / -> /v1
	return nil
}

func (*service) APIVersionIndex(dependencies.ForPublicRequest) (res *buffer.ServiceDetail, err error) {
	res = &ServiceDetail{
		API:           "buffer",
		Documentation: "https://buffer.keboola.com/v1/documentation",
	}
	return res, nil
}

func (*service) HealthCheck(dependencies.ForPublicRequest) (res string, err error) {
	return "OK", nil
}

func (*service) CreateReceiver(d dependencies.ForProjectRequest, payload *buffer.CreateReceiverPayload) (res *buffer.Receiver, err error) {
	ctx := d.RequestCtx()

	store := d.ConfigStore()

	config := model.Receiver{
		ProjectID: d.ProjectID(),
		Name:      payload.Name,
	}

	// Generate receiver ID from Name if needed
	if payload.ReceiverID != nil {
		config.ID = *payload.ReceiverID
	} else {
		config.ID = strhelper.NormalizeName(config.Name)
	}

	// Generate Secret
	config.Secret = idgenerator.ReceiverSecret()

	// Persist receiver
	err = store.CreateReceiver(ctx, config)
	if err != nil {
		return nil, err
	}

	// nolint: godox
	// TODO: create exports

	url := fmt.Sprintf("https://%s/v1/import/%s/#/%s", d.BufferApiHost(), config.ID, config.Secret)
	resp := &buffer.Receiver{
		ReceiverID: &config.ID,
		Name:       &config.Name,
		URL:        &url,
		Exports:    []*Export{},
	}

	return resp, nil
}

func (*service) GetReceiver(dependencies.ForProjectRequest, *buffer.GetReceiverPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) ListReceivers(dependencies.ForProjectRequest, *buffer.ListReceiversPayload) (res []*buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) DeleteReceiver(dependencies.ForProjectRequest, *buffer.DeleteReceiverPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) RefreshReceiverTokens(dependencies.ForProjectRequest, *buffer.RefreshReceiverTokensPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) CreateExport(dependencies.ForProjectRequest, *buffer.CreateExportPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) UpdateExport(dependencies.ForProjectRequest, *buffer.UpdateExportPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) DeleteExport(dependencies.ForProjectRequest, *buffer.DeleteExportPayload) (res *buffer.Receiver, err error) {
	return nil, &NotImplementedError{}
}

func (*service) Import(dependencies.ForPublicRequest, *buffer.ImportPayload, io.ReadCloser) (err error) {
	return &NotImplementedError{}
}
