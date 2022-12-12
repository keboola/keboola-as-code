package key

import (
	"fmt"
	"time"
)

const TimeFormat = "2006-01-02T15:04:05.000Z"

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

type FileKey struct {
	ExportKey
	FileID time.Time `json:"fileId" validate:"required"`
}

type SliceKey struct {
	FileKey
	SliceID time.Time `json:"sliceId" validate:"required"`
}

type RecordKey struct {
	ExportKey
	SliceID      time.Time `json:"sliceId" validate:"required"`
	ReceivedAt   time.Time
	RandomSuffix string
}

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}

func NewRecordKey(projectID int, receiverID string, exportID string, sliceID time.Time, now time.Time) RecordKey {
	k := RecordKey{}
	k.ProjectID = projectID
	k.ReceiverID = receiverID
	k.ExportID = exportID
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

func (v FileKey) String() string {
	return fmt.Sprintf("%s/file:%s", v.ExportKey.String(), FormatTime(v.FileID))
}

func (v SliceKey) String() string {
	return fmt.Sprintf("%s/slice:%s", v.FileKey.String(), FormatTime(v.SliceID))
}

func (v RecordKey) Key() string {
	return FormatTime(v.ReceivedAt) + "_" + v.RandomSuffix
}

func (v RecordKey) String() string {
	return fmt.Sprintf("%s/record:%s", v.ExportKey.String(), v.Key())
}
