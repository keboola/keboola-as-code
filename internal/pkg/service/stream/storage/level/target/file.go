package target

import (
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging"
)

type File struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	// StorageJob is the last table import job.
	StorageJob *keboola.StorageJob `json:"storageJob,omitempty"`
}

func NewFile(cfg Config, tableID keboola.TableID, stagingFile staging.File) File {
	return File{
		TableID: tableID,
	}
}
