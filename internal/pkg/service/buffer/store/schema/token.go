package schema

import (
	"strconv"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type tokens = PrefixT[model.Token]

type Tokens struct {
	tokens
}

type TokensInExport struct {
	tokens
}

func (v SecretsRoot) Tokens() Tokens {
	return Tokens{tokens: NewTypedPrefix[model.Token](
		v.prefix.Add("export/token"),
		v.schema.serialization,
	)}
}

func (v Tokens) InExport(k storeKey.ExportKey) KeyT[model.Token] {
	return v.tokens.
		Add(strconv.Itoa(k.ProjectID)).
		Add(k.ReceiverID).
		Key(k.ExportID)
}
