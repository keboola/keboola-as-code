package definition

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/column"
)

const (
	SinkTypeTable = SinkType("table")
)

type TableSink struct {
	Storage          storage.ConfigPatch    `json:"storage,omitempty"`
	UploadConditions *SliceUploadConditions `json:"uploadConditions,omitempty"` // nil == default
	ImportConditions TableImportConditions  `json:"importConditions"`
	Mapping          TableMapping           `json:"mapping"`
}

// SliceUploadConditions struct configures conditions for slice upload to the staging storage.
type SliceUploadConditions struct {
	Count uint64            `json:"count" mapstructure:"count" usage:"Records count." validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" mapstructure:"size" usage:"Records size." validate:"minBytes=100B,maxBytes=50MB"`
	Time  time.Duration     `json:"time" mapstructure:"time" usage:"Duration from the last upload/import." validate:"minDuration=1s,maxDuration=30m"`
}

// TableImportConditions struct configures conditions for import of the sliced file to the target table.
type TableImportConditions struct {
	Count uint64            `json:"count" mapstructure:"count" usage:"Records count." validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" mapstructure:"size" usage:"Records size." validate:"minBytes=100B,maxBytes=500MB"`
	Time  time.Duration     `json:"time" mapstructure:"time" usage:"Duration from the last upload/import." validate:"minDuration=30s,maxDuration=24h"`
}

type TableMapping struct {
	TableID keboola.TableID `json:"tableId" validate:"required"`
	Columns column.Columns  `json:"columns" validate:"required,min=1,max=100,dive"`
}

type StorageToken = keboola.Token

// DefaultSliceUploadConditions determines when a file slice will be imported to the storage.
// These settings are configurable, see TableSink.UploadConditions,
// but usually the TableSink.UploadConditions value is nil and conditions are configured per stack by ENVs.
func DefaultSliceUploadConditions() SliceUploadConditions {
	return SliceUploadConditions{
		Count: 10000,
		Size:  1 * datasize.MB,
		Time:  30 * time.Second,
	}
}

// DefaultTableImportConditions determines when a sliced file will be imported to the table.
// These settings must be configurable, see TableSink.ImportConditions.
func DefaultTableImportConditions() TableImportConditions {
	return TableImportConditions{
		Count: 10000,
		Size:  5 * datasize.MB,
		Time:  5 * time.Minute,
	}
}
