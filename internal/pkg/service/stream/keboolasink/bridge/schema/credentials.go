package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type (
	// UploadCredentials is an etcd prefix that stores all Keboola Storage API file upload credential.
	UploadCredentials struct {
		etcdop.PrefixT[keboolasink.FileUploadCredentials]
	}
)

func forFileUploadCredentials(s *serde.Serde) UploadCredentials {
	return UploadCredentials{PrefixT: etcdop.NewTypedPrefix[keboolasink.FileUploadCredentials]("storage/keboola/file/upload/credentials", s)}
}

func (v UploadCredentials) ForFile(k model.FileKey) etcdop.KeyT[keboolasink.FileUploadCredentials] {
	return v.PrefixT.Key(k.String())
}
