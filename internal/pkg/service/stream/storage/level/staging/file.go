package staging

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local"
)

type File struct {
	// IsEmpty is set if the import was skipped because there is no slice in the file.
	IsEmpty bool `json:"isEmpty,omitempty"`
	// Compression configuration.
	Compression compression.Config `json:"compression"  `
	// UploadCredentials to the staging storage.
	UploadCredentials           *keboola.FileUploadCredentials `json:"credentials" validate:"required"`
	UploadCredentialsExpiration utctime.UTCTime                `json:"credentialsExpiration" validate:"required"`
}

func NewFile(localFile local.File, credentials *keboola.FileUploadCredentials) File {
	// Note: Compression in the staging storage is same as in the local storage, but it can be modified in the future.
	return File{
		Compression:                 localFile.Compression,
		UploadCredentials:           credentials,
		UploadCredentialsExpiration: utctime.From(credentials.CredentialsExpiration()),
	}
}
