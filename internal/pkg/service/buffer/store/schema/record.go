package schema

import (
	"strconv"
	"time"

	storeKey "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

func (v RecordsRoot) ByKey(k storeKey.RecordKey) Key {
	if k.ProjectID == 0 {
		panic(errors.New("record projectID cannot be empty"))
	}
	if k.ReceiverID == "" {
		panic(errors.New("record receiverID cannot be empty"))
	}
	if k.ExportID == "" {
		panic(errors.New("record exportID cannot be empty"))
	}
	if k.SliceID.IsZero() {
		panic(errors.New("record sliceID cannot be empty"))
	}
	if k.ReceivedAt == (time.Time{}) {
		panic(errors.New("record receivedAt cannot be empty"))
	}
	if k.RandomSuffix == "" {
		panic(errors.New("record randomSuffix cannot be empty"))
	}
	return v.records.
		Add(strconv.Itoa(k.ProjectID)).
		Add(k.ReceiverID).
		Add(k.ExportID).
		Add(storeKey.FormatTime(k.SliceID)).
		Key(k.Key())
}
