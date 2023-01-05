package key

import (
	jsonlib "encoding/json"
	"time"
)

// TimeFormat is a time format with fixed length, so it can be used for lexicographic sorting in etcd.
const TimeFormat = "2006-01-02T15:04:05.000Z"

type (
	// UTCTime serializes to the JSON as the UTC time in the TimeFormat.
	UTCTime    time.Time
	ReceivedAt UTCTime
)

func (v ReceivedAt) String() string {
	return UTCTime(v).String()
}

func (v ReceivedAt) IsZero() bool {
	return UTCTime(v).IsZero()
}

func (v ReceivedAt) After(target ReceivedAt) bool {
	return UTCTime(v).After(UTCTime(target))
}

func (v ReceivedAt) MarshalJSON() ([]byte, error) {
	return UTCTime(v).MarshalJSON()
}

func (v *ReceivedAt) UnmarshalJSON(b []byte) error {
	return (*UTCTime)(v).UnmarshalJSON(b)
}

func (v UTCTime) String() string {
	return FormatTime(time.Time(v))
}

func (v UTCTime) IsZero() bool {
	return time.Time(v).IsZero()
}

func (v UTCTime) After(target UTCTime) bool {
	return time.Time(v).After(time.Time(target))
}

func (v UTCTime) MarshalJSON() ([]byte, error) {
	return jsonlib.Marshal(v.String())
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
