package local

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/writer/disksync"
)

type File struct {
	// Dir defines slice directory in the data volume.
	Dir string `json:"dir" validate:"required"`
	// Compression of the local file.
	Compression compression.Config `json:"compression"`
	// Sync writer configuration.
	Sync disksync.Config `json:"sync"`
	// Volumes configures the assignment of pod volumes to the File.
	Volumes VolumesAssignment `json:"volumes"`
}

type VolumesAssignment struct {
	// PerPod defines the quantity of volumes simultaneously utilized per pod by the File.
	// This value also corresponds to the number of slices simultaneously opened per pod and the File.
	// If the specified number of volumes is unavailable, all available volumes will be used.
	// With the growing number of volumes, the per pod throughput increases.
	PerPod int `json:"perPod" validate:"min=1"`
	// PreferredTypes contains a list of preferred volume types,
	// the value is used when assigning volumes to the file slices, see writer.Volumes.VolumesFor.
	// The first value is the most preferred volume type.
	PreferredTypes []string `json:"prefTypes"`
}
