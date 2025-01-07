package keboola

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// File contains all Keboola-specific data we need for upload and import.
type File struct {
	FileKey               *model.FileKey
	SinkKey               key.SinkKey
	TableID               keboola.TableID
	Columns               []string
	StorageJobID          *keboola.StorageJobID
	UploadCredentials     *keboola.FileUploadCredentials
	EncryptedCredentials  []byte
	FileID                *keboola.FileID
	FileName              *string
	CredentialsExpiration *utctime.UTCTime
}

func (file File) ID() keboola.FileID {
	if file.EncryptedCredentials != nil {
		return *file.FileID
	}
	return file.UploadCredentials.FileID
}

func (file File) Name() string {
	if file.EncryptedCredentials != nil {
		return *file.FileName
	}
	return file.UploadCredentials.Name
}

func (file File) Expiration() utctime.UTCTime {
	if file.EncryptedCredentials != nil {
		return *file.CredentialsExpiration
	}
	return utctime.From(file.UploadCredentials.CredentialsExpiration())
}
