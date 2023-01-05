package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type exports = PrefixT[model.ExportBase]

type Exports struct {
	exports
}

type ExportsInReceiver struct {
	exports
}

func (v ConfigsRoot) Exports() Exports {
	return Exports{exports: NewTypedPrefix[model.ExportBase](
		v.prefix.Add("export"),
		v.schema.serde,
	)}
}

func (v Exports) InReceiver(k storeKey.ReceiverKey) ExportsInReceiver {
	return ExportsInReceiver{exports: v.exports.Add(k.String())}
}

func (v Exports) ByKey(k storeKey.ExportKey) KeyT[model.ExportBase] {
	return v.Key(k.String())
}
