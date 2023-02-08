package schema

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
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

func (v TaskLocks) InReceiver(receiverKey key.ReceiverKey) TaskLocksInReceiver {
	return TaskLocksInReceiver{prefix: v.prefix.Add(receiverKey.String())}
}

func (v TaskLocksInReceiver) LockKey(lockName string) etcdop.Key {
	return v.prefix.Key(lockName)
}
