// Package closesync provides synchronization between source and coordinators nodes regarding closing slices
// The coordinator nodes are waiting for slice pipeline to finish, the router nodes notify about closed slices.
package closesync

import (
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
)

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
	EtcdSerde() *serde.Serde
}

type schema struct {
	prefix etcdop.PrefixT[int64]
}

func newSchema(s *serde.Serde) schema {
	return schema{
		prefix: etcdop.NewTypedPrefix[int64]("runtime/closesync/source/node", s),
	}
}

func (s schema) SourceNode(sourceNodeID string) etcdop.KeyT[int64] {
	return s.prefix.Key(sourceNodeID)
}
