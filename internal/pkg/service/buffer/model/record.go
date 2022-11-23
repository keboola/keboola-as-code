package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/schema"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
)

type RecordKey struct {
	ProjectID  int
	ReceiverID string
	ExportID   string
	FileID     string
	SliceID    string
	ReceivedAt time.Time
}

func (k RecordKey) Key() Key {
	return schema.Records().
		InProject(k.ProjectID).
		InReceiver(k.ReceiverID).
		InExport(k.ExportID).
		InFile(k.FileID).
		InSlice(k.SliceID).
		ID(schema.FormatTimeForKey(k.ReceivedAt) + "_" + idgenerator.Random(5))
}

func (k RecordKey) String() string {
	return k.Key().Key()
}
