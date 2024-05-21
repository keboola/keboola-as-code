package staging

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
)

const (
	DefaultFileExpiration = 24 * time.Hour
)

type File struct {
	// IsEmpty is set if the import was skipped because there is no slice in the file.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// Provider of the staging file.
	Provider FileProvider `json:"provider" validate:"required"`
	// Compression configuration.
	Compression compression.Config `json:"compression" validate:"required"`
	// Expiration determines how long it is possible to write to the staging file, e.g. due to expiring credentials.
	Expiration utctime.UTCTime `json:"expiration" validate:"required"`
}

type FileProvider string

func NewFile(localFile local.File, openedAt time.Time) File {
	// Note: Compression in the staging storage is same as in the local storage, but it can be modified in the future.
	return File{
		Compression: localFile.Compression,
		Expiration:  utctime.From(openedAt.Add(DefaultFileExpiration)),
	}
}
