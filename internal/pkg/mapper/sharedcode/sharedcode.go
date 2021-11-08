package sharedcode

import (
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/codes"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func NewCodesMapper(context model.MapperContext) interface{} {
	return codes.NewMapper(context)
}

func NewVariablesMapper(context model.MapperContext) interface{} {
	return variables.NewMapper(context)
}
