package definition

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type SoftDeletableInterface interface {
	Delete(now time.Time, deletedWithParent bool)
	Undelete()
	IsDeleted() bool
	WasDeletedWithParent() bool
	GetDeletedAt() *utctime.UTCTime
}

type SoftDeletable struct {
	Deleted   bool             `json:"deleted,omitempty"`
	DeletedAt *utctime.UTCTime `json:"deletedAt,omitempty" validate:"required_if=Deleted true,excluded_if=Deleted false"`
	// DeletedWithParent is true when the object has not been deleted directly but was deleted together with its parent.
	DeletedWithParent bool `json:"deletedWithParent,omitempty" validate:"excluded_if=Deleted false"`
}

func (v *SoftDeletable) Delete(now time.Time, deletedWithParent bool) {
	at := utctime.From(now)
	v.Deleted = true
	v.DeletedAt = &at
	v.DeletedWithParent = deletedWithParent
}

func (v *SoftDeletable) Undelete() {
	v.Deleted = false
	v.DeletedWithParent = false
	v.DeletedAt = nil
}

func (v *SoftDeletable) IsDeleted() bool {
	return v.Deleted
}

func (v *SoftDeletable) WasDeletedWithParent() bool {
	return v.DeletedWithParent
}

func (v *SoftDeletable) GetDeletedAt() *utctime.UTCTime {
	return v.DeletedAt
}
