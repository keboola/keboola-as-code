package volume

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const IDLength = 10

type ID string

// Volume instance common interface.
type Volume interface {
	Path() string
	Type() string
	Label() string
	ID() ID
	Metadata() Metadata
	Close(ctx context.Context) error
}

// Spec provides base information about a volume found in volumes path.
type Spec struct {
	NodeID string `json:"nodeId" validate:"required"`
	Path   string `json:"path" validate:"required"`
	Type   string `json:"type" validate:"required"`
	Label  string `json:"label" validate:"required"`
}

// Metadata entity contains metadata about an active local volume that is connected to a writer/reader node.
type Metadata struct {
	VolumeID ID `json:"volumeId" validate:"required"`
	Spec
}

func GenerateID() ID {
	return ID(idgenerator.Random(IDLength))
}

func (v ID) String() string {
	if v == "" {
		panic(errors.New("ID cannot be empty"))
	}
	return string(v)
}
