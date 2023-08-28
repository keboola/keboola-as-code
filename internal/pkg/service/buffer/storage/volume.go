package storage

import (
	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const VolumeIDLength = 10

type VolumeID string

func GenerateVolumeID() VolumeID {
	return VolumeID(idgenerator.Random(VolumeIDLength))
}

func (v VolumeID) String() string {
	if v == "" {
		panic(errors.New("VolumeID cannot be empty"))
	}
	return string(v)
}
