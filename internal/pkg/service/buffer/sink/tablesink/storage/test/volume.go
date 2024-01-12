package test

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
)

type Volume struct {
	IDValue     storage.VolumeID
	NodeIDValue string
	PathValue   string
	TypeValue   string
	LabelValue  string
	CloseError  error
}

func NewTestVolume(id storage.VolumeID, nodeID string, info storage.VolumeSpec) *Volume {
	return &Volume{
		IDValue:     id,
		NodeIDValue: nodeID,
		PathValue:   info.Path,
		TypeValue:   info.Type,
		LabelValue:  info.Label,
	}
}

func (v *Volume) Path() string {
	return v.PathValue
}

func (v *Volume) Type() string {
	return v.TypeValue
}

func (v *Volume) Label() string {
	return v.LabelValue
}

func (v *Volume) ID() storage.VolumeID {
	return v.IDValue
}

func (v *Volume) Metadata() storage.VolumeMetadata {
	return storage.VolumeMetadata{
		VolumeID: v.IDValue,
		VolumeSpec: storage.VolumeSpec{
			Path:   v.PathValue,
			Type:   v.TypeValue,
			Label:  v.LabelValue,
			NodeID: v.NodeIDValue,
		},
	}
}

func (v *Volume) Close(_ context.Context) error {
	return v.CloseError
}
