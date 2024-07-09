package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
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
