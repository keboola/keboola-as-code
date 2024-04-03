package definition

import (
	"fmt"
	"time"

	"github.com/mitchellh/hashstructure/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type VersionedInterface interface {
	EntityCreatedAt() utctime.UTCTime
	EntityCreatedBy() By
	VersionNumber() VersionNumber
	VersionHash() string
	VersionModifiedAt() utctime.UTCTime
	VersionModifiedBy() By
	VersionDescription() string
}

type Versioned struct {
	CreatedAt utctime.UTCTime `json:"createdAt" hash:"ignore" validate:"required"`
	CreatedBy By              `json:"createdBy" hash:"ignore" validate:"required"`
	Version   Version         `json:"version"`
}

type Version struct {
	Number      VersionNumber   `json:"number" hash:"ignore" validate:"required,min=1"`
	Hash        string          `json:"hash" hash:"ignore" validate:"required,len=16"`
	ModifiedAt  utctime.UTCTime `json:"modifiedAt" hash:"ignore" validate:"required"`
	ModifiedBy  By              `json:"modifiedBy" hash:"ignore" validate:"required"`
	Description string          `json:"description" hash:"ignore"`
}

type VersionNumber int

func (v *Versioned) IncrementVersion(s any, now time.Time, by By, description string) {
	if v.CreatedAt.IsZero() {
		v.CreatedAt = utctime.From(now)
		v.CreatedBy = by
	}
	v.Version.ModifiedAt = utctime.From(now)
	v.Version.ModifiedBy = by
	v.Version.Description = description
	v.Version.Number += 1
	v.Version.Hash = hashStruct(s)
}

func (v *Versioned) EntityCreatedAt() utctime.UTCTime {
	return v.CreatedAt
}

func (v *Versioned) EntityCreatedBy() By {
	return v.CreatedBy
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

func (v *Versioned) VersionModifiedBy() By {
	return v.Version.ModifiedBy
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
