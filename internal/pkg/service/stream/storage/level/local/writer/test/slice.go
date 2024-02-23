package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func NewSlice() *model.Slice {
	return test.NewSlice()
}

func NewSliceOpenedAt(openedAt string) *model.Slice {
	return test.NewSliceOpenedAt(openedAt)
}
