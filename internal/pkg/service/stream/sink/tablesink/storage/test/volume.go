package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/tablesink/storage/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Volume struct {
	IDValue     volume.ID
	NodeIDValue string
	PathValue   string
	TypeValue   string
	LabelValue  string
	CloseError  error
}

// volumeRepository interface to prevent package import cycles.
type volumeRepository interface {
	RegisterWriterVolume(v volume.Metadata, leaseID etcd.LeaseID) op.WithResult[volume.Metadata]
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

func RegisterWriterVolumes(t *testing.T, ctx context.Context, volumeRepo volumeRepository, session *concurrency.Session, count int) {
	t.Helper()
	volumes := []volume.Metadata{
		{
			VolumeID: "my-volume-1",
			Spec:     volume.Spec{NodeID: "node-a", Type: "hdd", Label: "1", Path: "hdd/1"},
		},
		{
			VolumeID: "my-volume-2",
			Spec:     volume.Spec{NodeID: "node-b", Type: "ssd", Label: "2", Path: "ssd/2"},
		},
		{
			VolumeID: "my-volume-3",
			Spec:     volume.Spec{NodeID: "node-b", Type: "hdd", Label: "3", Path: "hdd/3"},
		},
		{
			VolumeID: "my-volume-4",
			Spec:     volume.Spec{NodeID: "node-b", Type: "ssd", Label: "4", Path: "ssd/4"},
		},
		{
			VolumeID: "my-volume-5",
			Spec:     volume.Spec{NodeID: "node-c", Type: "hdd", Label: "5", Path: "hdd/5"},
		},
	}

	if count < 1 || count > 5 {
		panic(errors.New("count must be 1-5"))
	}

	txn := op.Txn(session.Client())
	for _, vol := range volumes[:count] {
		txn.Merge(volumeRepo.RegisterWriterVolume(vol, session.Lease()))
	}
	require.NoError(t, txn.Do(ctx).Err())
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
