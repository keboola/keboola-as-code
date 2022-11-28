package key

import (
	"fmt"
	"time"
)

type ReceiverKey struct {
	ProjectID  int    `json:"projectId" validate:"required,min=1"`
	ReceiverID string `json:"receiverId" validate:"required,min=1,max=48"`
}

type ExportKey struct {
	ReceiverKey
	ExportID string `json:"exportId" validate:"required,min=1,max=48"`
}

type MappingKey struct {
	ExportKey
	RevisionID int `json:"revisionId" validate:"min=1,max=100"`
}

type SliceKey struct {
	ExportKey
	FileID  string
	SliceID string
}

type RecordKey struct {
	SliceKey
	ReceivedAt   time.Time
	RandomSuffix string
}

func FormatTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func NewRecordKey(projectID int, receiverID, exportID, fileID, sliceID string, now time.Time) RecordKey {
	k := RecordKey{}
	k.ProjectID = projectID
	k.ReceiverID = receiverID
	k.ExportID = exportID
	k.FileID = fileID
	k.SliceID = sliceID
	k.ReceivedAt = now
	return k
}

func (v ReceiverKey) String() string {
	return fmt.Sprintf("project:%d/receiver:%s", v.ProjectID, v.ReceiverID)
}

func (v ExportKey) String() string {
	return fmt.Sprintf("%s/export:%s", v.ReceiverKey.String(), v.ExportID)
}

func (v MappingKey) String() string {
	return fmt.Sprintf("%s/mapping:%08d", v.ExportKey.String(), v.RevisionID)
}

func (v SliceKey) String() string {
	return fmt.Sprintf("%s/file:%s/slice:%s", v.ExportKey.String(), v.FileID, v.SliceID)
}

func (v RecordKey) Key() string {
	return FormatTime(v.ReceivedAt) + "_" + v.RandomSuffix
}

func (v RecordKey) String() string {
	return fmt.Sprintf("%s/record:%s", v.SliceKey.String(), v.Key())
}
