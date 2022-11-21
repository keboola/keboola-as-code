package model

import (
	"fmt"
)

type TableID struct {
	Stage      string `json:"stage"`
	BucketName string `json:"bucketName"`
	TableName  string `json:"tableName"`
}

func (t TableID) String() string {
	return fmt.Sprintf("%s.c-%s.%s", t.Stage, t.BucketName, t.TableName)
}
