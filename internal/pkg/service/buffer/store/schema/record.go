package schema

import (
	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type records = Prefix

type RecordsRoot struct {
	records
}

type RecordsInProject struct {
	records
}

type RecordsInReceiver struct {
	records
}

type RecordsInExport struct {
	records
}

type RecordsInFile struct {
	records
}

type RecordsInSlice struct {
	records
}

func (v *Schema) Records() RecordsRoot {
	return RecordsRoot{records: NewPrefix("record")}
}

func (v RecordsRoot) InReceiver(k storeKey.ReceiverKey) RecordsInReceiver {
	return RecordsInReceiver{
		records: v.records.Add(k.String()),
	}
}

func (v RecordsRoot) InExport(k storeKey.ExportKey) RecordsInExport {
	return RecordsInExport{
		records: v.records.Add(k.String()),
	}
}

func (v RecordsRoot) InSlice(k storeKey.SliceKey) RecordsInSlice {
	return RecordsInSlice{
		records: v.records.Add(k.ExportKey.String()).Add(k.SliceID.String()),
	}
}

func (v RecordsRoot) ByKey(k storeKey.RecordKey) Key {
	return v.InSlice(k.SliceKey).Key(k.ID())
}
