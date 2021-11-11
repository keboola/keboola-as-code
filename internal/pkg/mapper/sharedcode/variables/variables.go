package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/helper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// mapper embeds variables config according "variables_id".
type mapper struct {
	model.MapperContext
	*helper.SharedCodeHelper
}

func NewMapper(context model.MapperContext) *mapper {
	return &mapper{MapperContext: context, SharedCodeHelper: helper.New(context.State, context.Naming)}
}
