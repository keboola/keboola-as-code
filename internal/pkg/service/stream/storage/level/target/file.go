package target

import (
	"github.com/keboola/go-client/pkg/keboola"
)

type Target struct {
	Table Table `json:"table"`
}

type Table struct {
	Keboola KeboolaTable `json:"keboola"`
}

type KeboolaTable struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	// StorageJob is the last table import job.
	StorageJob *keboola.StorageJob `json:"storageJob,omitempty"`
}

func New(tableID keboola.TableID) Target {
	return Target{
		Table: Table{
			Keboola: KeboolaTable{
				TableID: tableID,
			},
		},
	}
}
