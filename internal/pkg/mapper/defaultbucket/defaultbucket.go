package defaultbucket

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type defaultBucketMapper struct {
	model.MapperContext
}

func NewMapper(context model.MapperContext) *defaultBucketMapper {
	return &defaultBucketMapper{MapperContext: context}
}
