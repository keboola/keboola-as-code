package schema

import (
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type APINodes struct {
	prefix
}

type APIWatchers struct {
	prefix
}

// APISliceWatchers contains the latest revision of the slices prefix per API node.
// Using this, we can detect that all API nodes have switched to a new slice, and we can close and upload the old one.
type APISliceWatchers struct {
	prefix
}

func (v RuntimeRoot) APINodes() APINodes {
	return APINodes{prefix: v.prefix.Add("api/node")}
}

func (v APINodes) Watchers() APIWatchers {
	return APIWatchers{prefix: v.prefix.Add("watcher")}
}

func (v APIWatchers) SlicesRevision() APISliceWatchers {
	return APISliceWatchers{prefix: v.prefix.Add("slices/revision")}
}

func (v APISliceWatchers) Node(nodeID string) Key {
	if nodeID == "" {
		panic(errors.New("nodeID cannot be empty"))
	}
	return v.prefix.Key(nodeID)
}
