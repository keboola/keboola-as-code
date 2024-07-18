package model

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
)

type File struct {
	// Dir defines file directory in the data volume.
	Dir string `json:"dir" validate:"required"`
	// Assignment configures volumes assignment.
	Assignment assignment.Assignment `json:"assignment"`
	// Allocation configures pre-allocation of the filesystem free space.
	Allocation diskalloc.Config `json:"allocation"`
}
