package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type tokens = PrefixT[model.Token]

type Tokens struct {
	tokens
}

func (v SecretsRoot) Tokens() Tokens {
	return Tokens{tokens: NewTypedPrefix[model.Token](
		v.prefix.Add("export/token"),
		v.schema.serde,
	)}
}

func (v Tokens) InReceiver(k storeKey.ReceiverKey) PrefixT[model.Token] {
	return v.tokens.Add(k.String())
}

func (v Tokens) InExport(k storeKey.ExportKey) KeyT[model.Token] {
	return v.Key(k.String())
}
