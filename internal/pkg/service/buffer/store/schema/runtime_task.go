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

type TaskLocksInExport struct {
	prefix
}

func (v RuntimeRoot) Lock() Locks {
	return Locks{prefix: v.prefix.Add("lock")}
}

func (v Locks) Task() TaskLocks {
	return TaskLocks{prefix: v.prefix.Add("task")}
}

func (v TaskLocks) InExport(exportKey key.ExportKey) TaskLocksInExport {
	return TaskLocksInExport{prefix: v.prefix.Add(exportKey.String())}
}

func (v TaskLocksInExport) LockKey(lockName string) etcdop.Key {
	return v.prefix.Key(lockName)
}
