package staging

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type File struct {
	// Compression configuration.
	Compression compression.Config `json:"compression"  validate:"dive"`
	// IsEmpty is set if the import was skipped because there is no slice in the file.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// UploadCredentials to the staging storage.
	UploadCredentials           *keboola.FileUploadCredentials `json:"credentials" validate:"required"`
	UploadCredentialsExpiration utctime.UTCTime                `json:"credentialsExpiration" validate:"required"`
}
