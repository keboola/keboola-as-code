package schema

import (
	"strconv"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
		v.schema.serialization,
	)}
}

func (v Exports) ByKey(k storeKey.ExportKey) KeyT[model.ExportBase] {
	return v.InReceiver(k.ReceiverKey).ID(k.ExportID)
}

func (v Exports) InReceiver(k storeKey.ReceiverKey) ExportsInReceiver {
	if k.ProjectID == 0 {
		panic(errors.New("export projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("export receiverID cannot be empty"))
	}
	return ExportsInReceiver{exports: v.exports.Add(strconv.Itoa(k.ProjectID)).Add(k.ReceiverID)}
}

func (v ExportsInReceiver) ID(exportID string) KeyT[model.ExportBase] {
	if exportID == "" {
		panic(errors.New("export exportID cannot be empty"))
	}
	return v.exports.Key(exportID)
}
