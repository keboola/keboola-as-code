package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type RecordIDCounter struct {
	prefix
}

type RecordIDCounterInReceiver struct {
	prefix
}

func (v RuntimeRoot) LastRecordID() RecordIDCounter {
	return RecordIDCounter{prefix: v.prefix.Add("last/record/id")}
}

func (v RecordIDCounter) InReceiver(k storeKey.ReceiverKey) RecordIDCounterInReceiver {
	return RecordIDCounterInReceiver{prefix: v.prefix.Add(k.String())}
}

func (v RecordIDCounter) ByKey(k storeKey.ExportKey) Key {
	return v.Key(k.String())
}
