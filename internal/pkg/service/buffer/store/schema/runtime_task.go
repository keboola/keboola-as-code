package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type Locks struct {
	prefix
}

type TaskLocks struct {
	prefix
}

type TaskLocksInReceiver struct {
	prefix
}

func (v RuntimeRoot) Lock() Locks {
	return Locks{prefix: v.prefix.Add("lock")}
}

func (v Locks) Task() TaskLocks {
	return TaskLocks{prefix: v.prefix.Add("task")}
}

func (v TaskLocks) LockKey(lockName string) etcdop.Key {
	return v.prefix.Key(lockName)
}
