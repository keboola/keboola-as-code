package staging

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
)

type Slice struct {
	// Path to the slice in the staging File.
	Path string `json:"path" validate:"required"`
	// Compression configuration.
	Compression compression.Config `json:"compression"  validate:"dive"`
}
