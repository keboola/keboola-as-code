package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink"
)

type (
	// Token is an etcd prefix that stores all Keboola Storage API token entities.
	Token struct {
		etcdop.PrefixT[keboolasink.Token]
	}
)

func forToken(s *serde.Serde) Token {
	return Token{PrefixT: etcdop.NewTypedPrefix[keboolasink.Token]("storage/keboola/secret/token", s)}
}

func (v Token) ForSink(k key.SinkKey) etcdop.KeyT[keboolasink.Token] {
	return v.PrefixT.Key(k.String())
}
