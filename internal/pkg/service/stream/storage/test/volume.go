package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Volume struct {
	IDValue       model.ID
	NodeIDValue   string
	HostnameValue string
	PathValue     string
	TypeValue     string
	LabelValue    string
	CloseError    error
}

// volumeRepository interface to prevent package import cycles.
type volumeRepository interface {
	RegisterWriterVolume(v model.Metadata, leaseID etcd.LeaseID) op.WithResult[model.Metadata]
}

func NewTestVolume(id model.ID, nodeID string, info model.Spec) *Volume {
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
	volumes := []model.Metadata{
		{
			ID:   "my-volume-1",
			Spec: model.Spec{NodeID: "node-a", Hostname: "localhost", Type: "hdd", Label: "1", Path: "hdd/1"},
		},
		{
			ID:   "my-volume-2",
			Spec: model.Spec{NodeID: "node-b", Hostname: "localhost", Type: "ssd", Label: "2", Path: "ssd/2"},
		},
		{
			ID:   "my-volume-3",
			Spec: model.Spec{NodeID: "node-b", Hostname: "localhost", Type: "hdd", Label: "3", Path: "hdd/3"},
		},
		{
			ID:   "my-volume-4",
			Spec: model.Spec{NodeID: "node-b", Hostname: "localhost", Type: "ssd", Label: "4", Path: "ssd/4"},
		},
		{
			ID:   "my-volume-5",
			Spec: model.Spec{NodeID: "node-c", Hostname: "localhost", Type: "hdd", Label: "5", Path: "hdd/5"},
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

func (v *Volume) ID() model.ID {
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

func (v *Volume) Metadata() model.Metadata {
	return model.Metadata{
		ID: v.IDValue,
		Spec: model.Spec{
			Path:     v.PathValue,
			Type:     v.TypeValue,
			Label:    v.LabelValue,
			NodeID:   v.NodeIDValue,
			Hostname: v.HostnameValue,
		},
	}
}

func (v *Volume) Close(_ context.Context) error {
	return v.CloseError
}
