package schema

import . "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"

type ConfigsRoot struct {
	prefix
	schema *Schema
}

func (v *Schema) Configs() ConfigsRoot {
	return ConfigsRoot{prefix: NewPrefix("config"), schema: v}
}
