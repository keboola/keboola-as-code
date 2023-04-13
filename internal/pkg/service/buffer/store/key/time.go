package key

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type (
	ReceivedAt utctime.UTCTime
)

func (v ReceivedAt) String() string {
	return utctime.UTCTime(v).String()
}

func (v ReceivedAt) IsZero() bool {
	return utctime.UTCTime(v).IsZero()
}

func (v ReceivedAt) After(target ReceivedAt) bool {
	return utctime.UTCTime(v).After(utctime.UTCTime(target))
}

func (v ReceivedAt) MarshalJSON() ([]byte, error) {
	return utctime.UTCTime(v).MarshalJSON()
}

func (v *ReceivedAt) UnmarshalJSON(b []byte) error {
	return (*utctime.UTCTime)(v).UnmarshalJSON(b)
}
