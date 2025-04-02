package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	keboolasink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type (
	// File is an etcd prefix that stores all Keboola-specific data we need for upload and import.
	File struct {
		etcdop.PrefixT[keboolasink.File]
	}
)

func forFile(s *serde.Serde) File {
	return File{PrefixT: etcdop.NewTypedPrefix[keboolasink.File]("storage/keboola/file", s)}
}

func (v File) ForFile(k model.FileKey) etcdop.KeyT[keboolasink.File] {
	return v.Key(k.String())
}
