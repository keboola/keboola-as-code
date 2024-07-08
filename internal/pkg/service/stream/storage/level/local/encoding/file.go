package encoding

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// OutputOpener opens the network output for writing.
type OutputOpener func(sliceKey model.SliceKey) (writechain.File, error)

func DefaultFileOpener(sliceKey model.SliceKey) (writechain.File, error) {
	panic("todo")
}
