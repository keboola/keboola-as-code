package local

import (
	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
)

type Slice struct {
	// Dir defines slice directory in the data volume.
	// The Dir also contains a lock and other auxiliary files.
	Dir string `json:"dir" validate:"required"`
	// Filename of the Slice data file, in the Dir.
	Filename string `json:"filename" validate:"required"`
	// IsEmpty is set if the upload was skipped because we did not receive any data.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// AllocateSpace when creating the local file.
	AllocateSpace datasize.ByteSize `json:"allocateSpace"`
	// Compression of the local file.
	Compression compression.Config `json:"compression"`
	// Sync writer configuration.
	Sync disksync.Config `json:"sync"`
}
