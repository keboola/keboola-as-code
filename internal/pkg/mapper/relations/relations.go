package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type relationsMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *relationsMapper {
	return &relationsMapper{MapperContext: context}
}
