package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type mappings = PrefixT[model.Mapping]

type Mappings struct {
	mappings
}

type MappingsInReceiver struct {
	mappings
}

type MappingsInExport struct {
	mappings
}

func (v ConfigsRoot) Mappings() Mappings {
	return Mappings{mappings: NewTypedPrefix[model.Mapping](
		v.prefix.Add("mapping/revision"),
		v.schema.serde,
	)}
}

func (v Mappings) InReceiver(k storeKey.ReceiverKey) MappingsInReceiver {
	return MappingsInReceiver{mappings: v.mappings.Add(k.String())}
}

func (v Mappings) InExport(k storeKey.ExportKey) MappingsInExport {
	return MappingsInExport{mappings: v.Add(k.String())}
}

func (v Mappings) ByKey(k storeKey.MappingKey) KeyT[model.Mapping] {
	return v.Key(k.String())
}
