// Package model contains common code for reader.Volumes and writer.Volumes implementations.
//
// Volume relative path has the following format: "{type}/{label}".
//
// The type is later used when assigning volumes.
// Different use-cases may prefer a different type of volume.
//
// The label has no special meaning, volumes are identified by the unique volume.ID,
// which is read from the IDFile on the volume, if the file does not exist,
// it is generated and saved by the writer.Volume.
package model

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	IDLength = 10
	IDFile   = "volume-id"
)

// ID of the volume.
type ID string

// RemoteAddr of the disk writer node.
type RemoteAddr string

// Volume common interface.
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
	// NodeAddress is network address (<hostname>:<port>) of the node that manages the volume.
	// Value is filled in only for disk writer nodes.
	NodeAddress RemoteAddr `json:"nodeAddress" validate:"hostname_port"`
	Path        string     `json:"path" validate:"required"`
	Type        string     `json:"type" validate:"required"`
	Label       string     `json:"label" validate:"required"`
}

// Metadata entity contains metadata about an active local volume that is connected to a writer/reader node.
type Metadata struct {
	ID ID `json:"volumeId" validate:"required"`
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

func (v RemoteAddr) String() string {
	if v == "" {
		panic(errors.New("Remote address cannot be empty"))
	}
	return string(v)
}
