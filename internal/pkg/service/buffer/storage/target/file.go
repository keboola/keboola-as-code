package target

import "github.com/keboola/go-client/pkg/keboola"

type File struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	// StorageJob is the last table import job.
	StorageJob *keboola.StorageJob `json:"storageJob,omitempty"`
}
