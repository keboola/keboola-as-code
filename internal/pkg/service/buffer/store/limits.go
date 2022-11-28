package store

import (
	"github.com/c2h5oh/datasize"
)

const (
	MaxReceiversPerProject       = 100
	MaxExportsPerReceiver        = 20
	MaxMappingRevisionsPerExport = 100
	MaxImportRequestSizeInBytes  = 1 * datasize.MB
	MaxMappedCSVRowSizeInBytes   = 1 * datasize.MB
)
