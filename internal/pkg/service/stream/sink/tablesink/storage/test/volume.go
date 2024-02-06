package test

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
)

type Volume struct {
	IDValue     volume.ID
	NodeIDValue string
	PathValue   string
	TypeValue   string
	LabelValue  string
	CloseError  error
}

func NewTestVolume(id volume.ID, nodeID string, info volume.Spec) *Volume {
	return &Volume{
		IDValue:     id,
		NodeIDValue: nodeID,
		PathValue:   info.Path,
		TypeValue:   info.Type,
		LabelValue:  info.Label,
	}
}

func (v *Volume) ID() volume.ID {
	return v.IDValue
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

func (v *Volume) Metadata() volume.Metadata {
	return volume.Metadata{
		VolumeID: v.IDValue,
		Spec: volume.Spec{
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
