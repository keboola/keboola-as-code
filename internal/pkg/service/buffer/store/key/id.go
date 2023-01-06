package key

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	ProjectID  int
	ReceiverID string
	ExportID   string
	RevisionID int
	FileID     UTCTime
	SliceID    UTCTime
)

func (v ProjectID) String() string {
	if v == 0 {
		panic(errors.New("projectID cannot be empty"))
	}
	return fmt.Sprintf("%08d", v)
}

func (v ReceiverID) String() string {
	if v == "" {
		panic(errors.New("receiverID cannot be empty"))
	}
	return string(v)
}

func (v ExportID) String() string {
	if v == "" {
		panic(errors.New("exportID cannot be empty"))
	}
	return string(v)
}

func (v RevisionID) String() string {
	if v == 0 {
		panic(errors.New("revisionID cannot be empty"))
	}
	return fmt.Sprintf("%08d", v)
}

func (v FileID) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v FileID) String() string {
	if v.IsZero() {
		panic(errors.New("record fileID cannot be empty"))
	}
	return UTCTime(v).String()
}

func (v SliceID) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v SliceID) String() string {
	if v.IsZero() {
		panic(errors.New("record sliceID cannot be empty"))
	}
	return UTCTime(v).String()
}

func (v FileID) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v SliceID) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v *FileID) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}

func (v *SliceID) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}
