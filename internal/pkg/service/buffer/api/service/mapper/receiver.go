package mapper

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m Mapper) ReceiversPayload(receivers []model.Receiver) []*buffer.Receiver {
	out := make([]*buffer.Receiver, 0, len(receivers))
	for _, data := range receivers {
		out = append(out, m.ReceiverPayload(data))
	}
	return out
}

func (m Mapper) ReceiverPayload(model model.Receiver) *buffer.Receiver {
	return &buffer.Receiver{
		ID:          model.ReceiverID,
		URL:         formatReceiverURL(m.bufferAPIHost, model.ReceiverKey, model.Secret),
		Name:        model.Name,
		Description: model.Description,
		Exports:     m.ExportsPayload(model.Exports),
	}
}

func (m Mapper) CreateReceiverModel(projectID keboola.ProjectID, secret string, payload buffer.CreateReceiverPayload) (r model.Receiver, err error) {
	receiverBase := m.createReceiverBaseModel(projectID, secret, payload)

	exports := make([]model.Export, 0, len(payload.Exports))
	for _, exportData := range payload.Exports {
		export, err := m.createExportBaseModel(receiverBase.ReceiverKey, *exportData)
		if err != nil {
			return model.Receiver{}, err
		}

		mapping, err := m.CreateMappingModel(export.ExportKey, 1, *exportData.Mapping)
		if err != nil {
			return model.Receiver{}, err
		}

		// Persist export
		exports = append(exports, model.Export{
			ExportBase: export,
			Mapping:    mapping,
		})
	}

	return model.Receiver{
		ReceiverBase: receiverBase,
		Exports:      exports,
	}, nil
}

func (m Mapper) UpdateReceiverModel(receiver model.ReceiverBase, payload buffer.UpdateReceiverPayload) (r model.ReceiverBase, err error) {
	if payload.Name != nil {
		receiver.Name = *payload.Name
	}

	if payload.Description != nil {
		receiver.Description = *payload.Description
	}

	return receiver, nil
}

func (m Mapper) createReceiverBaseModel(projectID keboola.ProjectID, secret string, payload buffer.CreateReceiverPayload) model.ReceiverBase {
	// Generate receiver ID from Name if needed
	var id key.ReceiverID
	if payload.ID != nil && len(*payload.ID) != 0 {
		id = key.ReceiverID(strhelper.NormalizeName(string(*payload.ID)))
	} else {
		id = key.ReceiverID(strhelper.NormalizeName(payload.Name))
	}

	// Description is optional
	var description string
	if payload.Description != nil {
		description = *payload.Description
	}

	return model.ReceiverBase{
		ReceiverKey: key.ReceiverKey{
			ProjectID:  projectID,
			ReceiverID: id,
		},
		Name:        payload.Name,
		Description: description,
		Secret:      secret,
	}
}
