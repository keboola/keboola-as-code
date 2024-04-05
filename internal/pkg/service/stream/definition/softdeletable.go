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
	Deleted   *Deleted   `json:"deleted,omitempty" validate:"excluded_with=Undeleted"`
	Undeleted *Undeleted `json:"undeletedAt,omitempty" validate:"excluded_with=Deleted"`
}

type Deleted struct {
	By By              `json:"by" validate:"required"`
	At utctime.UTCTime `json:"at" validate:"required"`
	// DeletedWithParent is true when the object has not been deleted directly but was deleted together with its parent.
	DeletedWithParent bool `json:"deletedWithParent"`
}

type Undeleted struct {
	By By              `json:"by" validate:"required"`
	At utctime.UTCTime `json:"at" validate:"required"`
}

func (v *SoftDeletable) Delete(now time.Time, by By, deletedWithParent bool) {
	at := utctime.From(now)
	v.Deleted = &Deleted{By: by, At: at, DeletedWithParent: deletedWithParent}
	v.Undeleted = nil

}

func (v *SoftDeletable) Undelete(now time.Time, by By) {
	at := utctime.From(now)
	v.Deleted = nil
	v.Undeleted = &Undeleted{By: by, At: at}
}

func (v *SoftDeletable) IsDeleted() bool {
	return v.Deleted != nil
}

func (v *SoftDeletable) WasDeletedWithParent() bool {
	return v.Deleted != nil && v.Deleted.DeletedWithParent
}

func (v *SoftDeletable) EntityDeletedBy() *By {
	if v.Deleted == nil {
		return nil
	}
	value := v.Deleted.By
	return &value
}

func (v *SoftDeletable) EntityDeletedAt() *utctime.UTCTime {
	if v.Deleted == nil {
		return nil
	}
	value := v.Deleted.At
	return &value
}

func (v *SoftDeletable) EntityUndeletedBy() *By {
	if v.Deleted == nil {
		return nil
	}
	value := v.Undeleted.By
	return &value
}

func (v *SoftDeletable) EntityUndeletedAt() *utctime.UTCTime {
	if v.Deleted == nil {
		return nil
	}
	value := v.Undeleted.At
	return &value
}
