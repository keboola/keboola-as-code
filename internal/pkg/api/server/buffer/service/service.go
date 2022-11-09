package service

import (
	"fmt"
	"io"
	"strconv"

	"github.com/iancoleman/strcase"
	dependencies "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/buffer/gen/buffer"
	. "github.com/keboola/keboola-as-code/internal/pkg/api/server/common/service"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/rand"
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

	store, err := d.ConfigStore(ctx)
	if err != nil {
		return nil, err
	}

	config := model.Receiver{
		ProjectID: strconv.Itoa(d.ProjectID()),
		Name:      payload.Name,
	}

	// Generate receiver ID from Name if needed
	if payload.ReceiverID != nil {
		config.ID = *payload.ReceiverID
	} else {
		config.ID = strcase.ToKebab(config.Name)
	}

	// Generate Secret
	config.Secret = rand.RandomString(32)

	// Persist receiver
	err = store.CreateReceiver(ctx, config)
	if err != nil {
		return nil, err
	}

	// TODO: create exports

	url := fmt.Sprintf("https://buffer.%s/v1/import/%s/#/%s", d.StorageApiHost(), config.ID, config.Secret)
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
