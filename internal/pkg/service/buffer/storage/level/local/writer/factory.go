package writer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/base"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/level/local/writer/csv"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Factory func(w *base.Writer) (SliceWriter, error)

func DefaultFactory(w *base.Writer) (SliceWriter, error) {
	// Create writer according to the file type
	switch w.Type() {
	case storage.FileTypeCSV:
		return csv.NewWriter(w)
	default:
		return nil, errors.Errorf(`unexpected file type "%s"`, w.Type())
	}
}
