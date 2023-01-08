package schema

import . "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"

type RuntimeRoot struct {
	prefix
}

func (v *Schema) Runtime() RuntimeRoot {
	return RuntimeRoot{prefix: NewPrefix("runtime")}
}
