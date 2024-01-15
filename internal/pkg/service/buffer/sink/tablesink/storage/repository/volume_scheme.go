package repository

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/volume"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type (
	// volumeSchema is an etcd prefix that stores metadata about active volumes.
	volumeSchema struct{ PrefixT[Metadata] }
)

func newVolumeSchema(s *serde.Serde) volumeSchema {
	return volumeSchema{PrefixT: NewTypedPrefix[Metadata]("storage/volume", s)}
}

func (v volumeSchema) WriterVolumes() PrefixT[Metadata] {
	return v.PrefixT.Add("writer")
}

func (v volumeSchema) ReaderVolumes() PrefixT[Metadata] {
	return v.PrefixT.Add("reader")
}

func (v volumeSchema) WriterVolume(id ID) KeyT[Metadata] {
	return v.WriterVolumes().Key(id.String())
}

func (v volumeSchema) ReaderVolume(id ID) KeyT[Metadata] {
	return v.ReaderVolumes().Key(id.String())
}
