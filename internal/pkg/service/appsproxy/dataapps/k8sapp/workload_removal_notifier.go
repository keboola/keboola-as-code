package k8sapp

import "sync"

// workloadRemovalNotifier tells subscribers that a workload left the cache, so a
// consumer keyed by WorkloadRef can drop what it holds for it.
type workloadRemovalNotifier struct {
	lock        sync.RWMutex
	subscribers []func(WorkloadRef)
}

func (n *workloadRemovalNotifier) subscribe(fn func(WorkloadRef)) {
	n.lock.Lock()
	defer n.lock.Unlock()
	n.subscribers = append(n.subscribers, fn)
}

// notify must be called with no watcher lock held: a subscriber may re-enter the watcher.
func (n *workloadRemovalNotifier) notify(ref WorkloadRef) {
	n.lock.RLock()
	subscribers := n.subscribers
	n.lock.RUnlock()

	for _, fn := range subscribers {
		fn(ref)
	}
}
