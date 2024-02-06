// Package watcher provides cache for API nodes and synchronization between API/Worker nodes.
//
// # API Node
//
// For the API node, the package provides a configuration cache (of receivers and slices),
// so it is possible to quickly process requests received by the import endpoint.
//
// See the NewAPINode function and APINode.GetReceiver method.
//
// # Worker Node
//
// For the Worker node, the package provides acknowledgment that a file slice
// is no longer used by any API node, because all API nodes switched to a new slice or gone away.
// This is done by watching the current revision of each API nodes.
//
// The Uploader can then start upload,
// because is guaranteed that no new records will be added to the slice prefix.
//
// See the NewWorkerNode function and WorkerNode.WaitForRevision method.
//
// # How the API Node works
//
// - The API node watches changes regarding receivers and slices via etcd Watch API.
// - The state is stored in the prefixtree.TreeThreadSafe structure, in the memory.
// - Based on this, the API node can check the secret and determine target prefix for the record (derived from the current key.SliceKey).
// - No query to the etcd is needed, all required values are cached, so import endpoint is fast and lock free.
// - Slice can be closed on a configuration change or if the upload conditions are met.
// - In that case, it is necessary to switch the API node to the new records prefix (derived from the new key.SliceKey).
// - It takes a while until all API nodes receive update about the slice/prefix change.
// - During this time, the Worker node cannot start upload of the slice, because new records can still be added.
// - The API node therefore reports its current revision (version of the cached state) to its own etcd key.
//
// # How the Worker Node works
//
// - The Worker node watches updates of the current revision of each API node via etcd Watch API.
// - On each change, the Worker node determines the minimum revision that match all API nodes.
// - The Uploader waits until all API nodes are synchronized to a required revision.
// - See WorkerNode.WaitForRevision method.
//
// # Example
//
// - Configuration change occurred or upload conditions are met.
// - Slice "A" is switched from "opened" to "closing" state.
// - A new slice "B" is created in the "opened" state.
// - Both changes are made atomically, within one etcd revision.
// - The changes increased the etcd revision number to "123".
// - API nodes watch for the change and acknowledge it by sending their current revision.
// - The worker node waits for all API nodes to send "123".
// - Upload of the slice "A" can then start.
// - Slice "A" is switched from "closing" to "uploading" state by the Uploader.
//
// # Resistance to outages
//
// - The etcd key, used by the API node to indicate its current revision, is created with etcd Lease (TTL mechanism).
// - When the API node loses connection with the etcd server, the etcd server waits by default revision.DefaultSessionTTL seconds.
// - The TTl value can be modified, see revision.WithTTL function.
// - After this time, the key is deleted and the Worker nodes receive the update.
//
// Example
// - Some unavailable API node last reported revision "100".
// - For upload, a revision "123" is needed.
// - There are 2 other API nodes that are already in proper revision.
// - The Worker node see [123, 100, 123], minimal revision is "100", upload is blocked.
// - By the Lease TTL mechanism, the key belonging to the unavailable node is deleted by the etcd server.
// - The Worker node see [123, 123], minimal revision is "123", upload can start.
package watcher

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/watcher/apinode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/watcher/workernode"
)

type (
	APINode    = apinode.Node
	WorkerNode = workernode.Node
)

func NewAPINode(d apinode.Dependencies, opts ...apinode.Option) (*APINode, error) {
	return apinode.New(d, opts...)
}

func NewWorkerNode(d workernode.Dependencies) (*WorkerNode, error) {
	return workernode.New(d)
}
