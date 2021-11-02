package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type variablesMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *variablesMapper {
	return &variablesMapper{MapperContext: context}
}
