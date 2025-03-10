package schema

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
)

type (
	// Volume is an etcd prefix that stores metadata about active volumes.
	Volume struct{ PrefixT[volume.Metadata] }
)

func New(s *serde.Serde) Volume {
	return Volume{PrefixT: NewTypedPrefix[volume.Metadata]("storage/volume", s)}
}

func (v Volume) WriterVolumes() PrefixT[volume.Metadata] {
	return v.PrefixT.Add("writer")
}

func (v Volume) ReaderVolumes() PrefixT[volume.Metadata] {
	return v.PrefixT.Add("reader")
}

func (v Volume) WriterVolume(id volume.ID) KeyT[volume.Metadata] {
	return v.WriterVolumes().Key(id.String())
}

func (v Volume) ReaderVolume(id volume.ID) KeyT[volume.Metadata] {
	return v.ReaderVolumes().Key(id.String())
}
