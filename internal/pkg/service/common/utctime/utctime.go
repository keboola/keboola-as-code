package utctime

import (
	jsonLib "encoding/json"
	"time"
)

// TimeFormat is a time format with fixed length, so it can be used for lexicographic sorting in etcd.
const TimeFormat = "2006-01-02T15:04:05.000Z"

type (
	// UTCTime serializes to the JSON as the UTC time in the TimeFormat.
	UTCTime time.Time
)

func (v UTCTime) String() string {
	return FormatTime(time.Time(v))
}

func (v UTCTime) IsZero() bool {
	return time.Time(v).IsZero()
}

func (v UTCTime) Time() time.Time {
	return time.Time(v)
}

func (v UTCTime) After(target UTCTime) bool {
	return v.Time().After(time.Time(target))
}

func (v UTCTime) MarshalJSON() ([]byte, error) {
	return jsonLib.Marshal(v.String())
}

func (v *UTCTime) UnmarshalJSON(b []byte) error {
	var str string
	if err := jsonLib.Unmarshal(b, &str); err != nil {
		return err
	}
	out, err := time.Parse(TimeFormat, str)
	if err != nil {
		return err
	}
	*v = UTCTime(out)
	return nil
}

func FormatTime(t time.Time) string {
	return t.UTC().Format(TimeFormat)
}
