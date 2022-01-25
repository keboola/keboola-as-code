package jsonnetfiles

import (
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
)

type jsonNetMapper struct {
	variables jsonnet.VariablesValues
}

func NewMapper(variables jsonnet.VariablesValues) *jsonNetMapper {
	return &jsonNetMapper{variables: variables}
}
