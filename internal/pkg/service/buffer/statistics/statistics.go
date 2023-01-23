// Package statistics provides collecting of statistics from the API node import endpoint
// and resolving of the upload and import conditions in the Worker node.

package statistics

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics/apinode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics/workernode"
)

type (
	APINode    = apinode.Node
	WorkerNode = workernode.Node
)

func NewAPINode(d apinode.Dependencies) *APINode {
	return apinode.New(d)
}

func NewWorkerNode(d workernode.Dependencies) (*WorkerNode, error) {
	return workernode.New(d)
}
