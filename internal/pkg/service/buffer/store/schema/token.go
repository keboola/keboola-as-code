package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type tokens = PrefixT[model.TokenForExport]

type Tokens struct {
	tokens
}

func (v SecretsRoot) Tokens() Tokens {
	return Tokens{tokens: NewTypedPrefix[model.TokenForExport](
		v.prefix.Add("export/token"),
		v.schema.serde,
	)}
}

func (v Tokens) InReceiver(k storeKey.ReceiverKey) PrefixT[model.TokenForExport] {
	if k.ProjectID == 0 {
		panic(errors.New("export token projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("export token receiverID cannot be empty"))
	}
	return v.tokens.Add(k.ProjectID.String()).Add(k.ReceiverID.String())
}

func (v Tokens) InExport(k storeKey.ExportKey) KeyT[model.TokenForExport] {
	if k.ExportID == "" {
		panic(errors.New("export token exportID cannot be empty"))
	}
	return v.InReceiver(k.ReceiverKey).Key(k.ExportID.String())
}
