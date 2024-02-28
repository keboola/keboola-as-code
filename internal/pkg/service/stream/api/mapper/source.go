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
		entity.SourceID = *payload.SourceID
	}

	// Name
	entity.Name = payload.Name

	// Description is optional
	if payload.Description != nil {
		entity.Description = *payload.Description
	}

	// Type
	entity.Type = payload.Type
	switch payload.Type {
	case definition.SourceTypeHTTP:
		entity.HTTP = &definition.HTTPSource{
			Secret: idgenerator.StreamHTTPSourceSecret(),
		}
	default:
		return definition.Source{}, svcerrors.NewBadRequestError(errors.Errorf(`unexpected "type" "%s"`, payload.Type.String()))
	}

	return entity, nil
}
