package mapper

import (
	"context"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m Mapper) ConditionsModel(payload *buffer.Conditions) (conditions model.Conditions, err error) {
	conditions = model.DefaultImportConditions()
	if payload != nil {
		conditions.Count = uint64(payload.Count)
		conditions.Size, err = datasize.ParseString(payload.Size)
		if err != nil {
			return model.Conditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`invalid conditions: value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
				payload.Size,
			))
		}
		conditions.Time, err = time.ParseDuration(payload.Time)
		if err != nil {
			return model.Conditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`invalid conditions: value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
				payload.Size,
			))
		}
	}

	if err := m.validator.Validate(context.Background(), conditions); err != nil {
		return conditions, serviceError.NewBadRequestError(errors.Errorf(`invalid conditions: %w`, err))
	}

	return conditions, nil
}
