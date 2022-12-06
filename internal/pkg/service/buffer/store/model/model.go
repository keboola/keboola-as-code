package model

import (
	"fmt"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model/column"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TableStageIn  = "in"
	TableStageOut = "out"
	TableStageSys = "sys"
)

type Receiver struct {
	ReceiverBase
	Exports []Export `validate:"dive"`
}

type Export struct {
	ExportBase
	Mapping Mapping `validate:"dive"`
	Token   Token   `validate:"dive"`
}

type ReceiverBase struct {
	key.ReceiverKey
	Name   string `json:"name" validate:"required,min=1,max=40"`
	Secret string `json:"secret" validate:"required,len=48"`
}

type ExportBase struct {
	key.ExportKey
	Name             string           `json:"name" validate:"required,min=1,max=40"`
	ImportConditions ImportConditions `json:"importConditions" validate:"required"`
}

type Token = storageapi.Token

type TokenForExport struct {
	key.ExportKey
	Token `validate:"dive"`
}

type Mapping struct {
	key.MappingKey
	TableID     TableID        `json:"tableId" validate:"required"`
	Incremental bool           `json:"incremental"`
	Columns     column.Columns `json:"columns" validate:"required,min=1,max=50"`
}

type ImportConditions struct {
	Count int               `json:"count" validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" validate:"min=100,max=50000000"`               // 100B-50MB
	Time  time.Duration     `json:"time" validate:"min=30000000000,max=86400000000000"` // 30s-24h
}

func (c ImportConditions) ShouldImport(s CurrentImportState) (bool, string) {
	if s.Count == 0 {
		return false, "no data to import"
	}

	defaults := DefaultConditions()
	if c.Count == 0 {
		c.Count = defaults.Count
	}
	if c.Size == 0 {
		c.Size = defaults.Size
	}
	if c.Time == 0 {
		c.Time = defaults.Time
	}

	if s.Count >= c.Count {
		return true, fmt.Sprintf("import count limit met, received: %d rows, limit: %d rows", s.Count, c.Count)
	}
	if s.Size >= c.Size {
		return true, fmt.Sprintf("import size limit met, received: %s, limit: %s", s.Size.String(), c.Size.String())
	}
	sinceLastImport := s.Now.Sub(s.LastImportAt).Truncate(time.Second)
	if sinceLastImport >= c.Time {
		return true, fmt.Sprintf("import time limit met, last import at: %s, passed: %s limit: %s", s.LastImportAt.Format(time.Stamp), sinceLastImport.String(), c.Time.String())
	}
	return false, "conditions not met"
}

type CurrentImportState struct {
	Count        int
	Size         datasize.ByteSize
	Now          time.Time
	LastImportAt time.Time
}

type TableID struct {
	Stage  string `json:"stage" validate:"required,oneof=in out sys"`
	Bucket string `json:"bucketName" validate:"required,min=1,max=96"`
	Table  string `json:"tableName" validate:"required,min=1,max=96"`
}

// nolint:gochecknoglobals
var tableStagesMap = map[string]bool{
	TableStageIn:  true,
	TableStageOut: true,
	TableStageSys: true,
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.c-%s.%s", t.Stage, t.Bucket, t.Table)
}

func (t TableID) BucketID() string {
	return fmt.Sprintf("%s.c-%s", t.Stage, t.Bucket)
}

func DefaultConditions() ImportConditions {
	return ImportConditions{
		Count: 1000,
		Size:  1 * datasize.MB,
		Time:  5 * time.Minute,
	}
}

func ParseTableID(v string) (TableID, error) {
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return TableID{}, errors.Errorf(`invalid table ID "%s"`, v)
	}

	stage, bucket, table := parts[0], parts[1], parts[2]

	if !tableStagesMap[stage] {
		return TableID{}, errors.Errorf(`invalid table ID "%s"`, v)
	}

	if !strings.HasPrefix(bucket, "c-") {
		return TableID{}, errors.Errorf(`invalid table ID "%s"`, v)
	}
	bucket = strings.TrimPrefix(bucket, "c-")

	return TableID{
		Stage:  stage,
		Bucket: bucket,
		Table:  table,
	}, nil
}

type File struct {
	key.FileKey
	Mapping             Mapping          `json:"mapping" validate:"required,dive"`
	StorageResource     *storageapi.File `json:"storageResource" validate:"required"`
	ClosedAt            string           `json:"closedAt,omitempty"`
	AllSlicesUploadedAt string           `json:"allSlicesUploadedAt,omitempty"`
	ManifestUploadedAt  string           `json:"manifestUploadedAt,omitempty"`
	ImportStartedAt     string           `json:"importStartedAt,omitempty"`
	ImportFinishedAt    string           `json:"importFinishedAt,omitempty"`
}

type Slice struct {
	key.SliceKey
	SliceNumber      int    `json:"sliceNumber" validate:"required"`
	ClosedAt         string `json:"closedAt,omitempty"`
	UploadStartedAt  string `json:"importStartedAt,omitempty"`
	UploadFinishedAt string `json:"importFinishedAt,omitempty"`
}
