package schema

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type WorkerNodes struct {
	prefix
}

type ActiveWorkers struct {
	prefix
}

// IDs prefix contains IDs of all active worker nodes.
// This is used by task distribution using the hash ring approach.
type IDs struct {
	prefix
}

type NodeID struct {
	prefix
}

func (v RuntimeRoot) WorkerNodes() WorkerNodes {
	return WorkerNodes{prefix: v.prefix.Add("worker/node")}
}

func (v WorkerNodes) Active() ActiveWorkers {
	return ActiveWorkers{prefix: v.prefix.Add("active")}
}

func (v ActiveWorkers) IDs() IDs {
	return IDs{prefix: v.prefix.Add("id")}
}

func (v IDs) Node(nodeID string) Key {
	if nodeID == "" {
		panic(errors.New("nodeID cannot be empty"))
	}
	return v.prefix.Key(nodeID)
}
