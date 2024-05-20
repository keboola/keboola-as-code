package definition

import (
	"fmt"
	"time"

	"github.com/mitchellh/hashstructure/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type VersionedInterface interface {
	VersionNumber() VersionNumber
	VersionHash() string
	VersionModifiedAt() utctime.UTCTime
	VersionModifiedBy() By
	VersionDescription() string
}

type Versioned struct {
	Version Version `json:"version"`
}

type Version struct {
	Number      VersionNumber   `json:"number" hash:"ignore" validate:"required,min=1"`
	Hash        string          `json:"hash" hash:"ignore" validate:"required,len=16"`
	Description string          `json:"description" hash:"ignore"`
	At          utctime.UTCTime `json:"at" hash:"ignore" validate:"required"`
	By          By              `json:"by" hash:"ignore" validate:"required"`
}

type VersionNumber int

func (v *Versioned) IncrementVersion(s any, now time.Time, by By, description string) {
	v.Version.At = utctime.From(now)
	v.Version.By = by

	v.Version.Number += 1
	v.Version.Hash = hashStruct(s)
	v.Version.Description = description
}

func (v *Versioned) VersionNumber() VersionNumber {
	return v.Version.Number
}

func (v *Versioned) VersionHash() string {
	return v.Version.Hash
}

func (v *Versioned) VersionModifiedAt() utctime.UTCTime {
	return v.Version.At
}

func (v *Versioned) VersionModifiedBy() By {
	return v.Version.By
}

func (v *Versioned) VersionDescription() string {
	return v.Version.Description
}

func (v VersionNumber) String() string {
	return fmt.Sprintf("%010d", v)
}

func hashStruct(s any) string {
	opts := &hashstructure.HashOptions{
		IgnoreZeroValue: true, // improve forward compatibility
	}

	if intHash, err := hashstructure.Hash(s, hashstructure.FormatV2, opts); err == nil {
		return fmt.Sprintf(`%016x`, intHash)
	} else {
		panic(err)
	}
}
