package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test"
)

func NewSlice() *storage.Slice {
	return test.NewSlice()
}

func NewSliceOpenedAt(openedAt string) *storage.Slice {
	return test.NewSliceOpenedAt(openedAt)
}
