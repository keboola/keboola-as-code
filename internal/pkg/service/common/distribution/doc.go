// Package distribution provides a mechanism to ensure that only one node
// in a cluster has ownership or control over a specific task or resource
// to prevent conflicts, data corruption, or duplication of work.
//
// The resource is represented by a string key.
// The owner of the key is determined locally within the node,
// without the need for unnecessary communication with other cluster nodes.
// The mechanism is implemented using the etcd database and the hash ring algorithm.
//
// # etcd
//   - The list of active nodes of a group is maintained in the etcd database.
//   - There can be several independent groups. Group name is an argument of the [NewNode] function.
//   - Each [Node] in the group has a unique etcd key.
//   - The key format is: runtime/distribution/group/<group>/nodes/<node_id>
//   - The value format is: <node_id>
//
// Node registration/deregistration:
//   - Each node registers the own key during startup, see the key format above.
//   - During a graceful shutdown the node removes the etcd key.
//   - The [etcd lease] (keep alive) mechanism automatically removes records of failed or disconnected nodes.
//   - For more information see the [Node] struct, it implements registration and watch workflows.
//
// Discovering of other nodes in the group is implemented in the [Node].watch method,
// it reads all records from the etcd prefix and watch for changes.
//
// # Hash Ring pattern
//
// The [Assigner] struct (embedded to the [Node] struct) implements local decision and assignment of a key to
// a specific node via hash ring pattern.
//
// The [Hash Ring pattern] (or Consistent Hashing) is a distributed system design that uses a ring structure
// and hash function to efficiently partition and balance data across nodes without centralized coordination.
//
// The [lafikl/consistent] library is used internally, is a lightweight implementation of the pattern,
// for example usage see the TestConsistentHashLib.
//
// # Benefits
//
//   - The Node only watch other node's registration/deregistration, which doesn't happen often.
//   - Based on this, the Node can quickly and locally determine the owner node for the key.
//   - It aims to reduce the risk of collision and minimizes load.
//
// # Atomicity
//
// During watch propagation or lease timeout, individual nodes can have a different list of the active nodes.
//
// 2+ owners problem:
//   - The above can lead to the situation, when 2+ nodes have ownership of a key at the same time.
//   - Therefore, an operation based on the key must be also protected by a transaction or distributed lock.
//   - Locks are outside the scope of this package, they are provided by the "task" package.
//
// 0 owners problem:
//   - The above can lead to the situation, when no node has ownership of a key.
//   - In other words, all running nodes are convinced that the ownership has a node that is no longer running,
//     but they have not been notified about change yet.
//   - It can sometimes be ignored, for example for periodic tasks, if one skipped interval is not a problem.
//   - Or it can be covered by application logic in another way, for example by a [Node.OnChangeListener].
//
// Read more:
//   - https://etcd.io/docs/v3.6/learning/why/#notes-on-the-usage-of-lock-and-lease
//   - "Actually, the lease mechanism itself doesn't guarantee mutual exclusion...."
//
// # Listeners
//
// Use [Node.OnChangeListener] method to create a listener for nodes distribution change events.
//
// [etcd lease]: https://etcd.io/docs/v3.6/learning/api/#lease-api
// [Hash Ring pattern]: https://www.youtube.com/watch?v=UF9Iqmg94tk
// [lafikl/consistent]: https://github.com/lafikl/consistent
package distribution
