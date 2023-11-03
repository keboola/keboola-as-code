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
	VersionDescription() string
}

type Versioned struct {
	Version Version `json:"version"`
}

type Version struct {
	Number      VersionNumber   `json:"number" hash:"ignore" validate:"required,min=1"`
	Hash        string          `json:"hash" hash:"ignore" validate:"required,len=16"`
	ModifiedAt  utctime.UTCTime `json:"modifiedAt" hash:"ignore" validate:"required"`
	Description string          `json:"description" hash:"ignore"`
}

type VersionNumber int

func (v *Versioned) IncrementVersion(s any, now time.Time, description string) {
	v.Version.ModifiedAt = utctime.From(now)
	v.Version.Description = description
	v.Version.Number += 1
	v.Version.Hash = hashStruct(s)
}

func (v *Versioned) VersionNumber() VersionNumber {
	return v.Version.Number
}

func (v *Versioned) VersionHash() string {
	return v.Version.Hash
}

func (v *Versioned) VersionModifiedAt() utctime.UTCTime {
	return v.Version.ModifiedAt
}

func (v *Versioned) VersionDescription() string {
	return v.Version.Description
}

func (v VersionNumber) String() string {
	return fmt.Sprintf("%010d", v)
}

func hashStruct(s any) string {
	intHash, err := hashstructure.Hash(s, hashstructure.FormatV2, &hashstructure.HashOptions{})
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf(`%016x`, intHash)
}
