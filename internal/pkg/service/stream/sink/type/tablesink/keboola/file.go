package keboola

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

// File contains all Keboola-specific data we need for upload and import.
type File struct {
	SinkKey           key.SinkKey
	TableID           keboola.TableID
	Columns           []string
	StorageJobID      *keboola.StorageJobID
	UploadCredentials keboola.FileUploadCredentials
}
