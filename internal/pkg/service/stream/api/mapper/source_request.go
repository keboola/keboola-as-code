package mapper

import (
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (m *Mapper) NewSourceEntity(parent key.BranchKey, payload *api.CreateSourcePayload) (definition.Source, error) {
	entity := definition.Source{}
	entity.BranchKey = parent

	// Generate source ID from Name if not set
	if payload.SourceID == nil || len(*payload.SourceID) == 0 {
		entity.SourceID = key.SourceID(strhelper.NormalizeName(payload.Name))
	} else {
		entity.SourceID = key.SourceID(strhelper.NormalizeName(string(*payload.SourceID)))
	}
	if entity.SourceID == "" {
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`"sourceId" must not be empty`))
	}

	// Name
	entity.Name = payload.Name

	// Description is optional
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Type
	entity.Type = payload.Type
	switch entity.Type {
	case definition.SourceTypeHTTP:
		entity.HTTP = &definition.HTTPSource{
			Secret: idgenerator.StreamHTTPSourceSecret(),
		}
	case definition.SourceTypeOTLP:
		entity.OTLP = &definition.OTLPSource{
			Secret: idgenerator.StreamHTTPSourceSecret(),
		}
	default:
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}

func (m *Mapper) UpdateSourceEntity(entity definition.Source, payload *api.UpdateSourcePayload) (definition.Source, error) {
	// Name
	if payload.Name != nil {
		entity.Name = *payload.Name
	}

	// Description
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Type
	if payload.Type != nil {
		entity.Type = *payload.Type
	}

	// Type-specific updates. Only the block matching the active type is kept
	// so that switching type drops the previous type's stale secret/config and
	// the persisted entity always carries exactly one type-specific block.
	switch entity.Type {
	case definition.SourceTypeHTTP:
		if entity.HTTP == nil {
			entity.HTTP = &definition.HTTPSource{}
		}
		if entity.HTTP.Secret == "" {
			entity.HTTP.Secret = idgenerator.StreamHTTPSourceSecret()
		}
		entity.OTLP = nil
	case definition.SourceTypeOTLP:
		if entity.OTLP == nil {
			entity.OTLP = &definition.OTLPSource{}
		}
		if entity.OTLP.Secret == "" {
			entity.OTLP.Secret = idgenerator.StreamHTTPSourceSecret()
		}
		entity.HTTP = nil
	default:
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}
