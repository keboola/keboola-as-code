package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/test"
)

func NewSlice() *storage.Slice {
	return test.NewSlice()
}

func NewSliceOpenedAt(openedAt string) *storage.Slice {
	return test.NewSliceOpenedAt(openedAt)
}
