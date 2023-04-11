package key

import (
	"fmt"

	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type (
	ProjectID  int
	ReceiverID string
	ExportID   string
	RevisionID int
	FileID     commonKey.UTCTime
	SliceID    commonKey.UTCTime
)

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
	return commonKey.UTCTime(v).IsZero()
}

func (v FileID) String() string {
	if v.IsZero() {
		panic(errors.New("record fileID cannot be empty"))
	}
	return commonKey.UTCTime(v).String()
}

func (v SliceID) IsZero() bool {
	return commonKey.UTCTime(v).IsZero()
}

func (v SliceID) String() string {
	if v.IsZero() {
		panic(errors.New("record sliceID cannot be empty"))
	}
	return commonKey.UTCTime(v).String()
}

func (v FileID) MarshalJSON() ([]byte, error) {
	return commonKey.UTCTime(v).MarshalJSON()
}

func (v SliceID) MarshalJSON() ([]byte, error) {
	return commonKey.UTCTime(v).MarshalJSON()
}

func (v *FileID) UnmarshalJSON(b []byte) error {
	return (*commonKey.UTCTime)(v).UnmarshalJSON(b)
}

func (v *SliceID) UnmarshalJSON(b []byte) error {
	return (*commonKey.UTCTime)(v).UnmarshalJSON(b)
}
