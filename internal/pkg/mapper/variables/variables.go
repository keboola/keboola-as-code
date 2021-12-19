package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
)

type variablesMapper struct {
	mapper.Context
}

func NewMapper(context mapper.Context) *variablesMapper {
	return &variablesMapper{Context: context}
}
