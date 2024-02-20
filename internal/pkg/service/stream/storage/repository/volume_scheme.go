package repository

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

type (
	// volumeSchema is an etcd prefix that stores metadata about active volumes.
	volumeSchema struct{ PrefixT[volume.Metadata] }
)

func newVolumeSchema(s *serde.Serde) volumeSchema {
	return volumeSchema{PrefixT: NewTypedPrefix[volume.Metadata]("storage/volume", s)}
}

func (v volumeSchema) WriterVolumes() PrefixT[volume.Metadata] {
	return v.PrefixT.Add("writer")
}

func (v volumeSchema) ReaderVolumes() PrefixT[volume.Metadata] {
	return v.PrefixT.Add("reader")
}

func (v volumeSchema) WriterVolume(id volume.ID) KeyT[volume.Metadata] {
	return v.WriterVolumes().Key(id.String())
}

func (v volumeSchema) ReaderVolume(id volume.ID) KeyT[volume.Metadata] {
	return v.ReaderVolumes().Key(id.String())
}
