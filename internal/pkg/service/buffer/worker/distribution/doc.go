// Package distribution provides distribution of various keys/tasks between cluster nodes.
// The owner of the task is determined locally, in the node, without unnecessary communication in the cluster.
//
// The package consists of:
// - Registration of a node in the cluster as an etcd key (with lease), see Node.
// - Discovering of other nodes in the cluster by the etcd Watch API, see Node.
// - Local decision and assignment of a key/task to a specific node (by a consistent hash/HashRing approach), see Assigner.
// - Distribution change listeners, see Node.OnChangeListener.
//
// # Key benefits
//
// - The Node only watch other node's registration/un-registration, which doesn't happen often.
// - Based on this, the Node can quickly and locally determine the owner node for the key/task.
// - It aims to reduce the risk of collision and minimizes load.
//
// # Atomicity
//
// - During watch propagation or lease timeout, individual nodes can have a different list of the active nodes.
// - This could lead to the situation, when 2+ nodes have ownership of a key at the same time.
// - Therefore, the task based on the key must be also protected by a transaction or distributed lock.
// - Task locks are implemented in the "task" package.
//
// Read more:
// - https://etcd.io/docs/v3.5/learning/why/#notes-on-the-usage-of-lock-and-lease
// - "Actually, the lease mechanism itself doesn't guarantee mutual exclusion...."
//
// # Listeners
//
// Use Node.OnChangeListener method to create a listener for nodes distribution change events.
package distribution
