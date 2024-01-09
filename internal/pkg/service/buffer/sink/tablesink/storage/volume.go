package storage

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const VolumeIDLength = 10

type VolumeID string

// Volume instance common interface.
type Volume interface {
	Path() string
	Type() string
	Label() string
	ID() VolumeID
	Metadata() VolumeMetadata
	Close(ctx context.Context) error
}

// VolumeSpec provides base information about a volume found in volumes path.
type VolumeSpec struct {
	NodeID string `json:"nodeId" validate:"required"`
	Path   string `json:"path" validate:"required"`
	Type   string `json:"type" validate:"required"`
	Label  string `json:"label" validate:"required"`
}

// VolumeMetadata entity contains metadata about an active local volume that is connected to a writer/reader node.
type VolumeMetadata struct {
	VolumeID VolumeID `json:"volumeId" validate:"required"`
	VolumeSpec
}

func GenerateVolumeID() VolumeID {
	return VolumeID(idgenerator.Random(VolumeIDLength))
}

func (v VolumeID) String() string {
	if v == "" {
		panic(errors.New("VolumeID cannot be empty"))
	}
	return string(v)
}
