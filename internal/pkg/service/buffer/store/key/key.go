// Package key defines the hierarchy of objects in the Buffer API.
// Each key is also part of the object value in the database.
package key

import (
	jsonlib "encoding/json"
	"fmt"
	"strconv"
	"time"
)

// TimeFormat is a time format with fixed length, so it can be used for lexicographic sorting in etcd.
const TimeFormat = "2006-01-02T15:04:05.000Z"

// UTCTime serializes to the JSON as the UTC time in the TimeFormat.
type UTCTime time.Time

type (
	ProjectID  int
	ReceiverID string
	ExportID   string
	RevisionID int
	FileID     UTCTime
	SliceID    UTCTime
	ReceivedAt UTCTime
)

type ReceiverKey struct {
	ProjectID  ProjectID  `json:"projectId" validate:"required,min=1"`
	ReceiverID ReceiverID `json:"receiverId" validate:"required,min=1,max=48"`
}

type ExportKey struct {
	ReceiverKey
	ExportID ExportID `json:"exportId" validate:"required,min=1,max=48"`
}

type MappingKey struct {
	ExportKey
	RevisionID RevisionID `json:"revisionId" validate:"min=1,max=100"`
}

type FileKey struct {
	ExportKey
	FileID FileID `json:"fileId" validate:"required"`
}

type SliceKey struct {
	FileKey
	SliceID SliceID `json:"sliceId" validate:"required"`
}

type RecordKey struct {
	SliceKey
	ReceivedAt   ReceivedAt
	RandomSuffix string
}

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}

func NewRecordKey(sliceKey SliceKey, now time.Time) RecordKey {
	return RecordKey{SliceKey: sliceKey, ReceivedAt: ReceivedAt(now)}
}

func (v *ReceiverKey) String() string {
	return fmt.Sprintf("project:%d/receiver:%s", v.ProjectID, v.ReceiverID)
}

func (v ExportKey) String() string {
	return fmt.Sprintf("%s/export:%s", v.ReceiverKey.String(), v.ExportID)
}

func (v MappingKey) String() string {
	return fmt.Sprintf("%s/mapping:%08d", v.ExportKey.String(), v.RevisionID)
}

func (v ProjectID) String() string {
	return strconv.Itoa(int(v))
}

func (v ReceiverID) String() string {
	return string(v)
}

func (v ExportID) String() string {
	return string(v)
}

func (v RevisionID) String() string {
	return strconv.Itoa(int(v))
}

func (v FileKey) String() string {
	return fmt.Sprintf("%s/file:%s", v.ExportKey.String(), v.FileID.String())
}

func (v SliceKey) String() string {
	return fmt.Sprintf("%s/slice:%s", v.FileKey.String(), v.SliceID.String())
}

func (v RecordKey) Key() string {
	return v.ReceivedAt.String() + "_" + v.RandomSuffix
}

func (v RecordKey) String() string {
	return fmt.Sprintf("%s/record:%s", v.ExportKey.String(), v.Key())
}

func (v UTCTime) String() string {
	return FormatTime(time.Time(v))
}

func (v FileID) String() string {
	return UTCTime(v).String()
}

func (v SliceID) String() string {
	return UTCTime(v).String()
}

func (v ReceivedAt) String() string {
	return UTCTime(v).String()
}

func (v UTCTime) IsZero() bool {
	return time.Time(v).IsZero()
}

func (v FileID) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v SliceID) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v ReceivedAt) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v UTCTime) After(target UTCTime) bool {
	return time.Time(v).After(time.Time(target))
}

func (v ReceivedAt) After(target ReceivedAt) bool {
	return UTCTime(v).After(UTCTime(target))
}

func (v UTCTime) MarshalJSON() ([]byte, error) {
	return jsonlib.Marshal(v.String())
}

func (v FileID) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v SliceID) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v ReceivedAt) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v *UTCTime) UnmarshalJSON(b []byte) error {
	var str string
	if err := jsonlib.Unmarshal(b, &str); err != nil {
		return err
	}
	out, err := time.Parse(TimeFormat, str)
	if err != nil {
		return err
	}
	*v = UTCTime(out)
	return nil
}

func (v *FileID) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}

func (v *SliceID) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}

func (v *ReceivedAt) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}
