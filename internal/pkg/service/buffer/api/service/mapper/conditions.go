package mapper

import (
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	serviceError "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m Mapper) ConditionsModel(payload *buffer.Conditions) (r model.Conditions, err error) {
	conditions := model.DefaultImportConditions()
	if payload != nil {
		conditions.Count = uint64(payload.Count)
		conditions.Size, err = datasize.ParseString(payload.Size)
		if err != nil {
			return model.Conditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid buffer size in bytes. Allowed units: B, kB, MB. For example: "5MB"`,
				payload.Size,
			))
		}
		conditions.Time, err = time.ParseDuration(payload.Time)
		if err != nil {
			return model.Conditions{}, serviceError.NewBadRequestError(errors.Errorf(
				`value "%s" is not valid time duration. Allowed units: s, m, h. For example: "30s"`,
				payload.Size,
			))
		}
	}
	return conditions, nil
}
