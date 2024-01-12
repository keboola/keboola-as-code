package repository

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type (
	// volumeSchema is an etcd prefix that stores metadata about active volumes.
	volumeSchema struct{ PrefixT[VolumeMetadata] }
)

func newVolumeSchema(s *serde.Serde) volumeSchema {
	return volumeSchema{PrefixT: NewTypedPrefix[VolumeMetadata]("storage/volume", s)}
}

func (v volumeSchema) WriterVolumes() PrefixT[VolumeMetadata] {
	return v.PrefixT.Add("writer")
}

func (v volumeSchema) ReaderVolumes() PrefixT[VolumeMetadata] {
	return v.PrefixT.Add("reader")
}

func (v volumeSchema) WriterVolume(id VolumeID) KeyT[VolumeMetadata] {
	return v.WriterVolumes().Key(id.String())
}

func (v volumeSchema) ReaderVolume(id VolumeID) KeyT[VolumeMetadata] {
	return v.ReaderVolumes().Key(id.String())
}
