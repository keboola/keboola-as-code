package writer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/base"
)

type Factory func(w base.Writer) (SliceWriter, error)
