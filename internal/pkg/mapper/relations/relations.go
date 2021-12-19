package relations

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
)

type relationsMapper struct {
	mapper.Context
}

func NewMapper(context mapper.Context) *relationsMapper {
	return &relationsMapper{Context: context}
}
