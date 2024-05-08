package staging

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
)

type File struct {
	// IsEmpty is set if the import was skipped because there is no slice in the file.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// Compression configuration.
	Compression compression.Config `json:"compression"`
	// Expiration determines how long it is possible to write to the staging file, e.g. due to expiring credentials.
	Expiration *utctime.UTCTime `json:"expiration"`
}

func NewFile(localFile local.File) File {
	// Note: Compression in the staging storage is same as in the local storage, but it can be modified in the future.
	return File{
		Compression: localFile.Compression,
	}
}
