package model

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	TableStageIn  = "in"
	TableStageOut = "out"
	TableStageSys = "sys"
)

// nolint:gochecknoglobals
var tableStagesMap = map[string]bool{
	TableStageIn:  true,
	TableStageOut: true,
	TableStageSys: true,
}

type TableID struct {
	Stage  string `json:"stage" validate:"required,oneof=in out sys"`
	Bucket string `json:"bucketName" validate:"required,min=1,max=96"`
	Table  string `json:"tableName" validate:"required,min=1,max=96"`
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.c-%s.%s", t.Stage, t.Bucket, t.Table)
}

func (t TableID) BucketID() string {
	return fmt.Sprintf("%s.c-%s", t.Stage, t.Bucket)
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
