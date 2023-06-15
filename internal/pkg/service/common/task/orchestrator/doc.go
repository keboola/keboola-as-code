// Package orchestrator provides a mechanism to start tasks based on events in an etcd prefix.
//
// The package combines the "task" and "distribution" packages to ensure that exactly one task
// runs across the entire cluster for each key in the configured etcd prefix.
//
// # Watched etcd Prefix
//
//   - The [Node.Start] method creates a new orchestartor based on the specified [Config].
//   - Etcd prefix is configured through the [Config].Source.WatchPrefix.
//   - All existing keys from the prefix are fetched during startup.
//   - New keys are continuously fetched from the etcd watcher as [etcdop.CreateEvent] events.
//   - It is expected that the triggering key will be deleted within the task if it is successful.
//   - See orchestrator.watch method for more details.
//
// # Starting Tasks
//
// The following workflow is performed for each fetched key from the etcd prefix:
//   - A task key is created using the [Config].TaskKey, result is a unique task identifier.
//   - A string distribution key is generated using the [Config].DistributionKey function.
//   - The distribution key determines whether the task should be started on the current node (see [distribution.Node]).
//   - Tasks with the same distribution key run on the same node.
//   - The [Config].StartTaskIf conditions is evaluated, if set.
//   - If all prerequisites are met ...
//   - A lock name is created using the [Config].Lock, if set, otherwise the task key is used as a fallback.
//   - A new task function is created using the [Config].TaskFactory.
//   - Finally, the task is started via [task.Node] - that ensures that only one task is running at a time.
//   - See orchestrator.startTask method and "task" package for details.
//
// # Periodical Restart
//
//   - Tasks can fail for various reasons, and in such cases, the trigger key still exists in the etcd prefix.
//   - After a certain time, it is necessary to retry the failed tasks.
//   - The [Config].Source.RestartInterval defines how often the watch stream will be restarted.
//   - On restart, the existing keys will be processed again, the workflow described in the "Watched etcd prefix" heading is repeated.
//   - Lock provided by the "task" package prevents duplicate execution if the task is still running.
//   - Use the [Config].StartTaskIf condition to implement a custom backoff if desired.
package orchestrator
