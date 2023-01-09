// Package key defines the hierarchy of objects in the Buffer API.
// Each key is also part of the object value in the database.
package key

import (
	"fmt"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

type TaskKey struct {
	ExportKey
	CreatedAt    UTCTime `json:"createdAt" validate:"required"`
	RandomSuffix string  `json:"-" validate:"required"`
}

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}

func NewRecordKey(sliceKey SliceKey, now time.Time) RecordKey {
	return RecordKey{SliceKey: sliceKey, ReceivedAt: ReceivedAt(now)}
}

func (v ReceiverKey) String() string {
	return fmt.Sprintf("%s/%s", v.ProjectID.String(), v.ReceiverID.String())
}

func (v ExportKey) String() string {
	return fmt.Sprintf("%s/%s", v.ReceiverKey.String(), v.ExportID.String())
}

func (v MappingKey) String() string {
	return fmt.Sprintf("%s/%s", v.ExportKey.String(), v.RevisionID.String())
}

func (v FileKey) String() string {
	return fmt.Sprintf("%s/%s", v.ExportKey.String(), v.FileID.String())
}

func (v SliceKey) String() string {
	return fmt.Sprintf("%s/%s", v.FileKey.String(), v.SliceID.String())
}

func (v RecordKey) String() string {
	return fmt.Sprintf("%s/%s/%s", v.ExportKey.String(), v.SliceID.String(), v.ID())
}

func (v RecordKey) ID() string {
	if v.ReceivedAt.IsZero() {
		panic(errors.New("receivedAt cannot be empty"))
	}
	if v.RandomSuffix == "" {
		panic(errors.New("randomSuffix cannot be empty"))
	}
	return v.ReceivedAt.String() + "_" + v.RandomSuffix
}

func (v TaskKey) String() string {
	return fmt.Sprintf("%s/%s", v.ExportKey.String(), v.ID())
}

func (v TaskKey) ID() string {
	if v.CreatedAt.IsZero() {
		panic(errors.New("createdAt cannot be empty"))
	}
	if v.RandomSuffix == "" {
		panic(errors.New("randomSuffix cannot be empty"))
	}
	return v.CreatedAt.String() + "_" + v.RandomSuffix
}
