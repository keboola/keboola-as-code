package model

import (
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/model/column"
)

const (
	TableStageIn  = "in"
	TableStageOut = "out"
	TableStageSys = "sys"
)

type TableID struct {
	Stage      string `json:"stage" validate:"required,oneof=in out sys"`
	BucketName string `json:"bucketName" validate:"required,min=1,max=96"`
	TableName  string `json:"tableName" validate:"required,min=1,max=96"`
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.c-%s.%s", t.Stage, t.BucketName, t.TableName)
}

type Mapping struct {
	RevisionID  int            `json:"revisionId" validate:"required"`
	TableID     TableID        `json:"tableId" validate:"required"`
	Incremental bool           `json:"incremental" validate:"required"`
	Columns     column.Columns `json:"columns" validate:"required,min=1,max=50"`
}

type Receiver struct {
	ID        string `json:"receiverId" validate:"required,min=1,max=48"`
	ProjectID int    `json:"projectId" validate:"required"`
	Name      string `json:"name" validate:"required,min=1,max=40"`
	Secret    string `json:"secret" validate:"required,len=48"`
}

type ImportCondition struct {
	Count int               `json:"count" validate:"min=1,max=10000000"`
	Size  datasize.ByteSize `json:"size" validate:"min=100,max=50000000"`
	Time  time.Duration     `json:"time" validate:"min=30000000000,max=86400000000000"`
}

type Export struct {
	ID               string            `json:"exportId" validate:"required,min=1,max=48"`
	Name             string            `json:"name" validate:"required,min=1,max=40"`
	ImportConditions []ImportCondition `json:"importConditions" validate:"required"`
}
