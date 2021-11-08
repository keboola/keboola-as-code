package variables

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// mapper embeds variables config according "variables_id".
type mapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *mapper {
	return &mapper{MapperContext: context}
}
