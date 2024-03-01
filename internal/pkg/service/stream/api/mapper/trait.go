package mapper

import (
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func (m *Mapper) NewVersionResponse(entity definition.Version) *api.Version {
	return &api.Version{
		Number:      entity.Number,
		Hash:        entity.Hash,
		Description: entity.Description,
		ModifiedAt:  entity.ModifiedAt.String(),
	}
}

func (m *Mapper) NewDeletedResponse(entity definition.SoftDeletable) *api.DeletedEntity {
	if !entity.Deleted {
		return nil
	}

	return &api.DeletedEntity{
		At: entity.DeletedAt.String(),
		By: nil,
	}
}

func (m *Mapper) NewDisabledResponse(entity definition.Switchable) *api.DisabledEntity {
	if !entity.Disabled {
		return nil
	}

	return &api.DisabledEntity{
		At:     entity.DisabledAt.String(),
		Reason: entity.DisabledReason,
		By:     nil,
	}
}
