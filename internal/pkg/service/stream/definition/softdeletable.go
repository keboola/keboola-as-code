package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SoftDeletableInterface interface {
	Delete(now time.Time, by By, deletedWithParent bool)
	Undelete(now time.Time, by By)
	IsDeleted() bool
	WasDeletedWithParent() bool
	EntityDeletedBy() *By
	EntityDeletedAt() *utctime.UTCTime
	EntityUndeletedBy() *By
	EntityUndeletedAt() *utctime.UTCTime
}

type SoftDeletable struct {
	Deleted     bool             `json:"deleted,omitempty"`
	DeletedBy   *By              `json:"deletedBy,omitempty" validate:"required_if=Deleted true,excluded_if=Deleted false"`
	DeletedAt   *utctime.UTCTime `json:"deletedAt,omitempty" validate:"required_if=Deleted true,excluded_if=Deleted false"`
	UndeletedBy *By              `json:"undeletedBy,omitempty" validate:"excluded_if=Deleted true"`
	UndeletedAt *utctime.UTCTime `json:"undeletedAt,omitempty" validate:"excluded_if=Deleted true"`
	// DeletedWithParent is true when the object has not been deleted directly but was deleted together with its parent.
	DeletedWithParent bool `json:"deletedWithParent,omitempty" validate:"excluded_if=Deleted false"`
}

func (v *SoftDeletable) Delete(now time.Time, by By, deletedWithParent bool) {
	at := utctime.From(now)
	v.Deleted = true
	v.DeletedBy = &by
	v.DeletedAt = &at
	v.UndeletedBy = nil
	v.UndeletedAt = nil
	v.DeletedWithParent = deletedWithParent
}

func (v *SoftDeletable) Undelete(now time.Time, by By) {
	at := utctime.From(now)
	v.Deleted = false
	v.DeletedBy = nil
	v.DeletedWithParent = false
	v.DeletedAt = nil
	v.UndeletedBy = &by
	v.UndeletedAt = &at
}

func (v *SoftDeletable) IsDeleted() bool {
	return v.Deleted
}

func (v *SoftDeletable) WasDeletedWithParent() bool {
	return v.DeletedWithParent
}

func (v *SoftDeletable) EntityDeletedBy() *By {
	return v.DeletedBy
}

func (v *SoftDeletable) EntityDeletedAt() *utctime.UTCTime {
	return v.DeletedAt
}

func (v *SoftDeletable) EntityUndeletedBy() *By {
	return v.UndeletedBy
}

func (v *SoftDeletable) EntityUndeletedAt() *utctime.UTCTime {
	return v.UndeletedAt
}
