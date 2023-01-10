// Package distribution provides distribution of various keys/tasks between worker nodes.
// The owner of the task is determined locally, in the worker node, without unnecessary communication in the cluster.
//
// The package consists of:
// - Registration of a worker node in the cluster as an etcd key (with lease), see Node.
// - Discovering of other worker nodes in the cluster by the etcd Watch API, see Node.
// - Local decision and assignment of a key/task to a specific worker node (by a consistent hash/HashRing approach), see Assigner.
// - Distribution change listeners, see Node.OnChangeListener.
// - The ExecutorWork restarted on each distribution change, see Node.StartExecutor.
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
// - This could lead to the situation, when 2+ nodes have ownership of a task at the same time.
// - Therefore, the task itself must be also protected by a transaction (version number validation).
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
