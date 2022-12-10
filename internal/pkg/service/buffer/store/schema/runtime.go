package schema

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type RuntimeRoot struct {
	prefix
}

type Workers struct {
	prefix
}

type ActiveWorkers struct {
	prefix
}

type IDs struct {
	prefix
}

type NodeID struct {
	prefix
}

func (v *Schema) Runtime() RuntimeRoot {
	return RuntimeRoot{prefix: NewPrefix("runtime")}
}

func (v RuntimeRoot) Workers() Workers {
	return Workers{prefix: v.prefix.Add("workers")}
}

func (v Workers) Active() ActiveWorkers {
	return ActiveWorkers{prefix: v.prefix.Add("active")}
}

func (v ActiveWorkers) IDs() IDs {
	return IDs{prefix: v.prefix.Add("ids")}
}

func (v IDs) Node(nodeID string) Key {
	if nodeID == "" {
		panic(errors.New("nodeID cannot be empty"))
	}
	return v.prefix.Key(nodeID)
}
