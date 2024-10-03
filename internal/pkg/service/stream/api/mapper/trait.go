package mapper

import (
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
)

func (m *Mapper) NewByResponse(by definition.By) *api.By {
	out := &api.By{
		Type: by.Type.String(),
	}

	if by.TokenID != "" {
		out.TokenID = &by.TokenID
	}

	if by.TokenDesc != "" {
		out.TokenDesc = &by.TokenDesc
	}

	if by.UserID != "" {
		out.UserID = &by.UserID
	}

	if by.UserName != "" {
		out.UserName = &by.UserName
	}

	return out
}

func (m *Mapper) NewVersionResponse(entity definition.Version) *api.Version {
	return &api.Version{
		Number:      entity.Number,
		Hash:        entity.Hash,
		Description: entity.Description,
		At:          entity.At.String(),
		By:          m.NewByResponse(entity.By),
	}
}

func (m *Mapper) NewVersionsResponse(versions []definition.Version) []*api.Version {
	var out []*api.Version
	for _, version := range versions {
		out = append(out, m.NewVersionResponse(version))
	}
	return out
}

func (m *Mapper) NewCreatedResponse(created definition.Created) *api.CreatedEntity {
	return &api.CreatedEntity{
		At: created.Created.At.String(),
		By: m.NewByResponse(created.Created.By),
	}
}

func (m *Mapper) NewDeletedResponse(entity definition.SoftDeletable) *api.DeletedEntity {
	if !entity.IsDeleted() {
		return nil
	}

	return &api.DeletedEntity{
		At: entity.DeletedAt().String(),
		By: m.NewByResponse(*entity.DeletedBy()),
	}
}

func (m *Mapper) NewDisabledResponse(entity definition.Switchable) *api.DisabledEntity {
	if !entity.IsDisabled() {
		return nil
	}

	return &api.DisabledEntity{
		At:     entity.DisabledAt().String(),
		Reason: entity.DisabledReason(),
		By:     m.NewByResponse(*entity.DisabledBy()),
	}
}
